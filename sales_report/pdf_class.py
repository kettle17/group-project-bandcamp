from datetime import date
from datetime import time
from fpdf import FPDF


class PDFReport(FPDF):
    def header(self):
        self.set_font("Helvetica", 'B', 16)
        self.cell(0, 10, "Daily BandCamp Sales Report", ln=True, align="C")
        self.ln(5)
        self.set_font("Helvetica", 'I', 12)
        self.cell(
            0, 10, f"Date report generated: {date.today().strftime('%B %d, %Y')}", ln=True, align="C")
        self.ln(10)

    def section_title(self, title):
        self.set_font("Helvetica", 'B', 14)
        self.set_text_color(0, 0, 128)
        self.cell(0, 10, title, ln=True)
        self.set_text_color(0, 0, 0)
        self.ln(2)

    def paragraph(self, text):
        self.set_font("Helvetica", '', 12)
        self.multi_cell(0, 8, text)
        self.ln(5)

    def insert_chart(self, path, caption=None):
        if caption:
            self.set_font("Helvetica", 'I', 11)
            self.multi_cell(0, 8, caption)
            self.image(path, w=170)
            self.ln(10)
