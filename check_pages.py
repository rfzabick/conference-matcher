"""Check if pypdf and PyMuPDF agree on page ordering."""
import os
import io
import fitz
import httpx
from pypdf import PdfReader

PRESENTATION_ID = os.environ.get("PRESENTATION_ID", "")
PDF_EXPORT_URL = f"https://docs.google.com/presentation/d/{PRESENTATION_ID}/export/pdf"

print("Downloading PDF...")
resp = httpx.get(PDF_EXPORT_URL, follow_redirects=True, timeout=120)
pdf_bytes = resp.content
print(f"PDF size: {len(pdf_bytes)} bytes")

doc = fitz.open(stream=pdf_bytes, filetype="pdf")
reader = PdfReader(io.BytesIO(pdf_bytes))

print(f"PyMuPDF pages: {len(doc)}")
print(f"pypdf pages:   {len(reader.pages)}")

if len(doc) != len(reader.pages):
    print(f"\n*** PAGE COUNT MISMATCH: offset = {len(reader.pages) - len(doc)} ***\n")

print("\n=== Text comparison (first 5 pages + pages 43-49) ===")
for i in list(range(min(5, len(doc)))) + list(range(43, min(50, len(doc)))):
    fitz_text = doc[i].get_text()[:100].replace('\n', ' ').strip()
    pypdf_text = reader.pages[i].extract_text()
    pypdf_text = (pypdf_text or "")[:100].replace('\n', ' ').strip()
    match = "MATCH" if fitz_text[:40] == pypdf_text[:40] else "MISMATCH"
    print(f"\nPage {i} [{match}]:")
    print(f"  fitz:  {fitz_text[:80]}")
    print(f"  pypdf: {pypdf_text[:80]}")

doc.close()
print("\nDone.")
