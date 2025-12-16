import requests
import json
from datetime import datetime
import os
import pymupdf as fitz  # for PDF processing
import time
import re

NAME = "Vlado Menkovski"
ROWS = 1000  # number of items per page

team = [
    "Simon Koop",
    "Koen Minartz",
    "Fleur Hendriks",
    "Marko Petkovic",
    "Marko Petković",
    "Mahdi Mehmanchi",
    "Kiet Bennema ten Brinke",
    "Rachna Ramesh",
    "Yoeri Poels",
]


def get_crossref_works():
    """Fetch all Crossref works"""
    base_url = "https://api.crossref.org/works"

    print("Fetching Crossref works...")

    params = {
        "query.author": NAME,
        "rows": ROWS,
        "sort": "issued",
        "order": "desc",
    }
    resp = requests.get(base_url, params=params)
    resp.raise_for_status()
    data = resp.json()["message"]

    items = data.get("items", [])

    print(f" → Retrieved {len(items)} items (total {len(items)})")

    return items


def get_datacite_works():
    base_url = "https://api.datacite.org/dois"
    print("Fetching DataCite works...")

    params = {
        "query": 'creators.name:"Menkovski, Vlado"',
        "page[size]": 1000,
    }
    resp = requests.get(base_url, params=params)
    resp.raise_for_status()
    data = resp.json()["data"]

    return data


def extract_crossref_data(item):
    """Extract publication data from Crossref format"""
    # Extract date info
    created = item.get("created", {}).get("date-time", "")
    date = datetime.strptime(created, "%Y-%m-%dT%H:%M:%SZ") if created else None
    year, month = (date.year, date.month) if date else (None, None)

    # Format month+year nicely
    month_name = datetime(1900, month, 1).strftime("%B") if month else None
    month_year = f"{month_name} {year}" if month_name else str(year)
    month_year_numeric = int(f"{year}{month:02d}") if year and month else None

    # Extract authors
    authors = []
    for a in item.get("author", []):
        authors.append({"given": a.get("given", ""), "family": a.get("family", "")})

    # Skip if Vlado Menkovski is not an author
    if not any(
        (a.get("given", "") == "Vlado" and a.get("family", "") == "Menkovski")
        for a in item.get("author", [])
    ):
        return None

    # Skip if none of the other authors are in the team
    if not any(
        any(
            (
                a.get("given", "") in member.split(" ")
                and a.get("family", "") in member.split(" ")
            )
            for member in team
        )
        for a in item.get("author", [])
    ):
        return None

    # Extract and clean abstract
    abstract_html = item.get("abstract", "")
    # Remove HTML tags if present
    abstract_text = re.sub(r"<.*?>", "", abstract_html).strip() if abstract_html else None

    return {
        "title": item.get("title", ["Untitled"])[0],
        "author": authors,
        "container-title": item.get("container-title", [""])[0],
        "DOI": item.get("DOI", ""),
        "issued": {
            "year": year,
            "month_year": month_year,
            "month_year_numeric": month_year_numeric,
        },
        "source": "crossref",
        "abstract": abstract_text,
    }


def extract_datacite_data(item):
    """Extract publication data from DataCite format"""
    attributes = item.get("attributes", {})

    # Extract date info
    created = attributes.get("created", "")
    date = datetime.strptime(created, "%Y-%m-%dT%H:%M:%SZ") if created else None
    year, month = (date.year, date.month) if date else (None, None)
    month_name = datetime(1900, month, 1).strftime("%B") if month else None
    month_year = f"{month_name} {year}" if month_name else str(year) if year else "Unknown"
    month_year_numeric = int(f"{year}{month:02d}") if year and month else None

    # Extract authors
    authors = []
    creators = attributes.get("creators", [])
    for creator in creators:
        name = creator.get("name", "")
        given_name = creator.get("givenName", "")
        family_name = creator.get("familyName", "")

        # Try to parse name if given/family not provided
        if not given_name and not family_name and name:
            # DataCite often uses "LastName, FirstName" format
            if ", " in name:
                parts = name.split(", ", 1)
                family_name = parts[0]
                given_name = parts[1] if len(parts) > 1 else ""
            else:
                # If no comma, assume "FirstName LastName"
                parts = name.split()
                if len(parts) > 1:
                    given_name = " ".join(parts[:-1])
                    family_name = parts[-1]
                else:
                    family_name = name

        authors.append({"given": given_name, "family": family_name})

    # Skip if Vlado Menkovski is not an author
    if not any(
        ("Vlado" in a.get("given", "") and "Menkovski" in a.get("family", "")) for a in authors
    ):
        return None

    # Skip if none of the other authors are in the team
    if not any(
        any(
            (
                a.get("given", "") in member.split(" ")
                and a.get("family", "") in member.split(" ")
            )
            for member in team
        )
        for a in authors
    ):
        return None

    # Extract DOI (remove URL prefix if present)
    doi = attributes.get("doi", "")
    if doi.startswith("https://doi.org/"):
        doi = doi.replace("https://doi.org/", "")

    # Get container title (journal/conference name)
    container = attributes.get("container", {})
    container_title = container.get("title", "") if isinstance(container, dict) else ""

    # If no container, try publisher
    if not container_title:
        container_title = attributes.get("publisher", "")

    # Extract abstract if available
    abstract = attributes.get("description", "")

    return {
        "title": attributes.get("titles", [{}])[0].get("title", "Untitled"),
        "author": authors,
        "container-title": container_title,
        "DOI": doi,
        "issued": {
            "year": year,
            "month_year": month_year,
            "month_year_numeric": month_year_numeric,
        },
        "source": "datacite",
        "abstract": abstract,
    }


def normalize_title(title):
    """Normalize title for comparison by removing punctuation, extra spaces, and converting to lowercase"""
    import re

    # Convert to lowercase
    title = title.lower()
    # Remove punctuation
    title = re.sub(r"[^\w\s]", "", title)
    # Remove extra whitespace
    title = " ".join(title.split())
    return title


def fetch_pdfs(dois):
    """Fetch PDFs for given DOIs using Unpaywall API"""
    base_url = "https://api.unpaywall.org/v2/"
    semantic_url = "https://api.semanticscholar.org/v1/paper/"
    email = "k.bennema.ten.brinke@tue.nl"

    for doi in dois:
        if f"_data/pdfs/{doi.replace('/', '_')}.pdf" in os.listdir("_data/pdfs/"):
            continue  # Skip if already downloaded
        url = f"{base_url}{doi}?email={email}"
        response = requests.get(url)
        r_json = response.json() if response.status_code == 200 else {}
        if response.status_code == 200 and r_json.get("best_oa_location") is not None:
            pdf_url = r_json.get("best_oa_location", {}).get("url_for_pdf")
            if pdf_url:
                try:
                    pdf_data = requests.get(pdf_url)
                    time.sleep(0.1)  # Be polite to the server
                except requests.exceptions.ConnectionError as e:
                    continue

                if pdf_data.status_code == 200:
                    with open(f"_data/pdfs/{doi.replace('/', '_')}.pdf", "wb") as f:
                        f.write(pdf_data.content)

        # Try Semantic Scholar as a fallback
        else:
            r = requests.get(semantic_url + doi)
            if r.status_code == 200:
                r_json = r.json()
                pdf_url = r_json.get("openAccessPdf", {}).get("url")
                if pdf_url:
                    pdf_data = requests.get(pdf_url)
                    time.sleep(0.1)  # Be polite to the server
                    if pdf_data.status_code == 200:
                        with open(f"_data/pdfs/{doi.replace('/', '_')}.pdf", "wb") as f:
                            f.write(pdf_data.content)

            else:
                print(f"Error fetching PDF for DOI {doi}: {response.status_code}")

    print(f"✅ {len(os.listdir('_data/pdfs/'))} PDFs fetched and saved to _data/pdfs/")


def get_pdf_thumbnails():
    """Convert the first page of each PDF to a PNG thumbnail using pdf2image"""

    pdfs = os.listdir("_data/pdfs/")

    for pdf_file in pdfs:
        if f"_data/thumbnails/{pdf_file.replace('.pdf', '.png')}" in os.listdir(
            "_data/thumbnails/"
        ):
            continue  # Skip if thumbnail already exists
        pdf_path = os.path.join("_data/pdfs/", pdf_file)
        doc = fitz.open(pdf_path)
        page = doc.load_page(0)  # Load first page
        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))  # Render at 2x resolution
        thumbnail_path = os.path.join("_data/thumbnails/", pdf_file.replace(".pdf", ".png"))
        pix.save(thumbnail_path)
        doc.close()

    print(
        f"✅ {len(os.listdir('_data/thumbnails/'))} Thumbnails generated and saved to _data/thumbnails/"
    )


def main():
    print("Fetching publications from Vlado from Crossref...")
    crossref_works = get_crossref_works()
    print(f"Retrieved {len(crossref_works)} works")

    datacite_works = get_datacite_works()
    print(f"Retrieved {len(datacite_works)} works from DataCite")

    # Use dictionaries to track unique DOIs and titles
    publications_by_doi = {}
    publications_by_title = {}

    # Collect all publications first
    all_pubs = []

    # Process Crossref works
    for item in crossref_works:
        pub = extract_crossref_data(item)
        if pub is not None:
            all_pubs.append(pub)

    # Process DataCite works
    for item in datacite_works:
        pub = extract_datacite_data(item)
        if pub is not None:
            all_pubs.append(pub)

    print(f"\nTotal publications before deduplication: {len(all_pubs)}")

    # Deduplicate by DOI and title, keeping the most recent
    for pub in all_pubs:
        doi = pub["DOI"].lower() if pub["DOI"] else None
        normalized_title = normalize_title(pub["title"])
        year = pub["issued"]["year"] if pub["issued"]["year"] else 0

        # Check if we should keep this publication
        keep = True

        # Check DOI duplicate
        if doi and doi in publications_by_doi:
            existing = publications_by_doi[doi]
            existing_year = existing["issued"]["year"] if existing["issued"]["year"] else 0
            # Keep the one with the most recent year, or prefer crossref if same year
            if year > existing_year or (year == existing_year and pub["source"] == "crossref"):
                # Replace with newer one
                publications_by_doi[doi] = pub
                # Also update in title dict if it was there
                if (
                    normalized_title in publications_by_title
                    and publications_by_title[normalized_title]["DOI"] == existing["DOI"]
                ):
                    publications_by_title[normalized_title] = pub
            keep = False  # Already handled
        elif doi:
            publications_by_doi[doi] = pub

        # Check title duplicate (only if not already handled by DOI)
        if keep and normalized_title:
            if normalized_title in publications_by_title:
                existing = publications_by_title[normalized_title]
                existing_year = existing["issued"]["year"] if existing["issued"]["year"] else 0
                # Keep the one with the most recent year, or prefer one with DOI, or prefer crossref
                if (
                    year > existing_year
                    or (year == existing_year and pub["DOI"] and not existing["DOI"])
                    or (
                        year == existing_year
                        and pub["source"] == "crossref"
                        and existing["source"] == "datacite"
                    )
                ):
                    publications_by_title[normalized_title] = pub
            else:
                publications_by_title[normalized_title] = pub

    final_publications = {}

    # Add title-based publications
    for title, pub in publications_by_title.items():
        final_publications[title] = pub

    # Convert to list
    publications = list(final_publications.values())

    # Sort by year (most recent first)
    publications.sort(
        key=lambda x: x["issued"]["month_year_numeric"]
        if x["issued"]["month_year_numeric"]
        else 0,
        reverse=True,
    )

    print("\n📊 Summary:")
    print(f"  - Total publications after deduplication: {len(publications)}")
    print(f"  - From Crossref: {len([p for p in publications if p['source'] == 'crossref'])}")
    print(f"  - From DataCite: {len([p for p in publications if p['source'] == 'datacite'])}")
    print(f"  - Duplicates removed: {len(all_pubs) - len(publications)}")

    # Save JSON for Jekyll
    with open("_data/publications-scraped.json", "w", encoding="utf-8") as f:
        json.dump(publications, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Saved {len(publications)} publications to _data/publications-scraped.json")

    # # Fetch PDFs
    # dois = [pub["DOI"] for pub in publications if pub["DOI"]]

    # print("\nFetching PDFs of publications...")
    # fetch_pdfs(dois)

    # print("\nGenerating PDF thumbnails...")
    # get_pdf_thumbnails()


if __name__ == "__main__":
    main()
