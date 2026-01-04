"""Parser para extraer contenido de páginas del SII."""
import re
from datetime import datetime
from typing import List, Optional, Tuple
from bs4 import BeautifulSoup

from .models import SIIDocument, DocumentType


class SIIParser:
    """Parser para documentos del SII."""

    @staticmethod
    def clean_text(text: str) -> str:
        """Limpia y normaliza texto extraído."""
        if not text:
            return ""
        # Remover espacios múltiples y líneas vacías
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'\n\s*\n', '\n\n', text)
        return text.strip()

    @staticmethod
    def extract_date(text: str) -> Optional[datetime]:
        """Extrae fecha de un texto."""
        patterns = [
            r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})',  # DD/MM/YYYY o DD-MM-YYYY
            r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})',  # YYYY/MM/DD o YYYY-MM-DD
            r'(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})',  # DD de Mes de YYYY
        ]

        months_es = {
            'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
            'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
            'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
        }

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                groups = match.groups()
                try:
                    if len(groups) == 3:
                        if pattern == patterns[0]:  # DD/MM/YYYY
                            return datetime(int(groups[2]), int(groups[1]), int(groups[0])).date()
                        elif pattern == patterns[1]:  # YYYY/MM/DD
                            return datetime(int(groups[0]), int(groups[1]), int(groups[2])).date()
                        elif pattern == patterns[2]:  # DD de Mes de YYYY
                            month = months_es.get(groups[1].lower())
                            if month:
                                return datetime(int(groups[2]), month, int(groups[0])).date()
                except (ValueError, TypeError):
                    continue
        return None

    @staticmethod
    def extract_doc_number(text: str, doc_type: DocumentType) -> Optional[str]:
        """Extrae número de documento según su tipo."""
        patterns = {
            DocumentType.CIRCULAR: r'Circular\s+N[°º]?\s*(\d+)',
            DocumentType.RESOLUCION: r'Resoluci[oó]n\s+(?:Ex[.]?)?\s*N[°º]?\s*(\d+)',
            DocumentType.LEY: r'Ley\s+N[°º]?\s*(\d+[.\d]*)',
            DocumentType.DECRETO: r'Decreto\s+(?:Ley\s+)?N[°º]?\s*(\d+)',
        }

        pattern = patterns.get(doc_type)
        if pattern:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    def parse_normativa_list(self, html: str, base_url: str) -> List[Tuple[str, str, str]]:
        """
        Parsea lista de normativas y retorna lista de (título, url, tipo).
        """
        soup = BeautifulSoup(html, 'html.parser')
        items = []

        # Buscar enlaces a documentos
        for link in soup.find_all('a', href=True):
            href = link['href']
            title = self.clean_text(link.get_text())

            if not title or len(title) < 5:
                continue

            # Construir URL completa si es relativa
            if not href.startswith('http'):
                href = base_url.rstrip('/') + '/' + href.lstrip('/')

            # Determinar tipo de documento
            doc_type = self._detect_doc_type(title, href)
            if doc_type:
                items.append((title, href, doc_type))

        return items

    def _detect_doc_type(self, title: str, url: str) -> Optional[str]:
        """Detecta el tipo de documento basado en título y URL."""
        text = (title + ' ' + url).lower()

        if 'circular' in text:
            return DocumentType.CIRCULAR.value
        elif 'resolucion' in text or 'resolución' in text:
            return DocumentType.RESOLUCION.value
        elif 'ley' in text:
            return DocumentType.LEY.value
        elif 'decreto' in text:
            return DocumentType.DECRETO.value
        elif 'formulario' in text:
            return DocumentType.FORMULARIO.value
        elif 'instructivo' in text:
            return DocumentType.INSTRUCTIVO.value

        return None

    def parse_document_page(self, html: str, url: str, doc_type: DocumentType) -> Optional[SIIDocument]:
        """
        Parsea una página de documento individual.
        """
        soup = BeautifulSoup(html, 'html.parser')

        # Remover scripts y estilos
        for element in soup(['script', 'style', 'nav', 'header', 'footer']):
            element.decompose()

        # Extraer título
        title = ""
        title_elem = soup.find('h1') or soup.find('h2') or soup.find('title')
        if title_elem:
            title = self.clean_text(title_elem.get_text())

        # Extraer contenido principal
        content = ""
        main_content = soup.find('main') or soup.find('article') or soup.find('div', class_='content')
        if main_content:
            content = self.clean_text(main_content.get_text())
        else:
            # Fallback: usar body completo
            body = soup.find('body')
            if body:
                content = self.clean_text(body.get_text())

        if not content or len(content) < 50:
            return None

        # Extraer metadatos
        doc_number = self.extract_doc_number(title + ' ' + content[:500], doc_type)
        published_date = self.extract_date(content[:1000])

        return SIIDocument(
            doc_type=doc_type,
            title=title,
            content=content,
            url=url,
            doc_number=doc_number,
            published_date=published_date,
            metadata={
                "content_length": len(content),
                "has_tables": bool(soup.find('table'))
            }
        )

    def parse_valores_utm_uf(self, html: str, url: str) -> List[SIIDocument]:
        """
        Parsea tabla de valores UTM/UF.
        """
        soup = BeautifulSoup(html, 'html.parser')
        documents = []

        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            if len(rows) < 2:
                continue

            # Extraer encabezados
            headers = []
            header_row = rows[0]
            for th in header_row.find_all(['th', 'td']):
                headers.append(self.clean_text(th.get_text()))

            # Extraer datos
            data_rows = []
            for row in rows[1:]:
                cells = [self.clean_text(td.get_text()) for td in row.find_all(['td', 'th'])]
                if cells:
                    data_rows.append(cells)

            if headers and data_rows:
                content = f"Encabezados: {', '.join(headers)}\n\n"
                for row in data_rows:
                    content += f"{' | '.join(row)}\n"

                doc = SIIDocument(
                    doc_type=DocumentType.TABLA_VALORES,
                    title=f"Tabla de Valores - {headers[0] if headers else 'SII'}",
                    content=content,
                    url=url,
                    metadata={
                        "headers": headers,
                        "row_count": len(data_rows)
                    }
                )
                documents.append(doc)

        return documents
