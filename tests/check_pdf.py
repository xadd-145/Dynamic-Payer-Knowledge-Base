import pdfplumber
from pathlib import Path

data_dir = Path('data/raw')
pdfs = list(data_dir.glob('*.pdf'))
print('PDFs found:', [p.name for p in pdfs])

for pdf_path in pdfs:
    print(f'\nTesting: {pdf_path.name}')
    with pdfplumber.open(str(pdf_path)) as pdf:
        print(f'  Pages: {len(pdf.pages)}')
        for i, page in enumerate(pdf.pages[:3], 1):
            text = page.extract_text(x_tolerance=3, y_tolerance=3)
            print(f'  Page {i} text length: {len(text) if text else 0}')
            if text:
                print(f'  First 100 chars: {text[:100]}')