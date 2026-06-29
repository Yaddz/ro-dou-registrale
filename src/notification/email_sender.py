"""Module for sending emails."""

import os
import sys

from tempfile import NamedTemporaryFile

import pandas as pd
from airflow.utils.email import send_email

# TODO fix this
# Add parent folder to sys.path in order to be able to import

# Add parent directory to sys.path
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


sys.path.insert(0, parent_dir)

from notification.isender import ISender
from notification.templateManager import TemplateManager

from schemas import ReportConfig


class EmailSender(ISender):
    """Prepare and send e-mails with the reports."""

    highlight_tags = ("<span class='highlight'>", "</span>")

    def __init__(self, report_config: ReportConfig) -> None:
        self.report_config = report_config
        self.search_report = ""
        self.watermark = ""

    def send(self, search_report: list, report_date: str):
        """Builds the email content, the CSV if applies, and send it"""
        self.search_report = search_report
        full_subject = f"{self.report_config.subject} - DOs de {report_date}"
        skip_notification = True
        for search in self.search_report:
            items = ["contains" for k, v in search["result"].items() if v]
            if items:
                skip_notification = False
            else:
                content = self._generate_email_content()

        if skip_notification:
            if self.report_config.skip_null:
                return "skip_notification"
        else:
            content = self._generate_email_content()

        if self.report_config.attach_csv and skip_notification is False:
            with self.get_csv_tempfile() as csv_file:
                send_email(
                    to=self.report_config.emails,
                    subject=full_subject,
                    files=[csv_file.name],
                    html_content=content,
                    mime_charset="utf-8",
                )
        else:
            send_email(
                to=self.report_config.emails,
                subject=full_subject,
                html_content=content,
                mime_charset="utf-8",
            )

    @staticmethod
    def clean_dou_text(text: str) -> str:
        """Remove lixo de navegação e formatação do DOU. Funciona para qualquer tipo de menção."""
        import re
        if not text: return ""
        
        text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"<p[^>]*>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"</p>", "", text, flags=re.IGNORECASE)
        text = re.sub(r"<div[^>]*>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"</div>", "", text, flags=re.IGNORECASE)
        text = re.sub(r"<[^>]+>", "", text)
        
        noise_lines = [
            r"^Ir para o conte[uú]do",
            r"^Ir para o rodap[eé]",
            r"^Logo do Governo Federal",
            r"^Órgãos do Governo",
            r"^Acesso [àa] Informação",
            r"^Legislação$",
            r"^Acessibilidade$",
            r"^Acesso GOV\.BR",
            r"^Imprensa Nacional$",
            r"^Página Inicial$",
            r"^Serviços$",
            r"^Diário Oficial da União$",
            r"^Compartilhe:",
            r"^Versão certificada",
            r"^Diário Completo$",
            r"^Impressão$",
            r"^Publicado em:",
            r"^Edição:\s*\d+",
            r"^Seção:\s*\d+",
            r"^Página:\s*\d+",
            r"^Órgão:",
            r"^RESOLUÇÃO.*DE \d{4}$",
            r"^Este conteúdo não substitui",
            r"^REPORTAR ERRO$",
            r"^logo gov\.br",
            r"^AUDIÊNCIA DO PORTAL$",
            r"^Páginas vistas$",
            r"^null$",
            r"^Visitantes únicos$",
            r"^Leitura do Jornal$",
            r"^Destaques do Diário",
            r"^Base de Dados",
            r"^Verificação de autenticidade",
            r"^Acesso ao sistema",
            r"^Concursos e Seleções",
            r"^Tutorial",
            r"^Termo de Uso",
            r"^Política de Privacidade",
            r"^Portal da Imprensa",
            r"^REDES SOCIAIS$",
            r"^Todo o conteúdo",
            r"^Conteúdo acessível",
            r"^VLibras Widget",
        ]
        
        lines = text.split("\n")
        cleaned_lines = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            skip = False
            for pattern in noise_lines:
                if re.match(pattern, line, re.IGNORECASE):
                    skip = True
                    break
            if not skip:
                cleaned_lines.append(line)
        
        text = "\n".join(cleaned_lines)
        
        text = re.sub(r"_{5,}", "\n---\n", text)
        text = re.sub(r"-{10,}", "\n---\n", text)
        text = re.sub(r"\n---\n\s*\n---\n", "\n---\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        
        return text.strip()

    @staticmethod
    def format_abstract(text: str, cnpj_log: str = "") -> str:
        import re, json, os
        if not text: return ""
        
        text = EmailSender.clean_dou_text(text)
        
        company_name = ""
        try:
            from src.models import Company
            target_norm = re.sub(r'[^A-Za-z0-9]', '', cnpj_log).upper()
            comp = Company.query.filter_by(cnpj_norm=target_norm).first()
            if comp: company_name = comp.nome
        except: pass

        clean_text = re.sub(r"<[^>]+>", "", text)
        clean_text = re.sub(r"\s*\.\.\.\s*", r"\n\n", clean_text)
        clean_text = re.sub(r"\s*-{10,}\s*", r"\n\n", clean_text)
        
        blocks = re.split(r'\n\n+', clean_text)
        
        relevant_blocks = []
        target_norm = re.sub(r'[^A-Za-z0-9]', '', cnpj_log).upper()
        
        for block in blocks:
            block_norm = re.sub(r'[^A-Za-z0-9]', '', block).upper()
            if target_norm and target_norm in block_norm:
                relevant_blocks.append(block)
            elif company_name and company_name.upper() in block.upper():
                relevant_blocks.append(block)

        if not relevant_blocks:
            relevant_blocks = blocks

        formatted_blocks = []
        for block in relevant_blocks:
            b = block.strip()
            if cnpj_log:
                b = re.sub(rf"({re.escape(cnpj_log)})", r"<b>\1</b>", b, flags=re.IGNORECASE)
            if company_name:
                b = re.sub(rf"({re.escape(company_name)})", r"<b>\1</b>", b, flags=re.IGNORECASE)
            
            if len(b) > 1000:
                match = re.search(r'<b>(.*?)</b>', b)
                if match:
                    start = max(0, match.start() - 300)
                    end = min(len(b), match.end() + 300)
                    b = ("..." if start > 0 else "") + b[start:end] + ("..." if end < len(b) else "")
                else:
                    b = b[:500] + "..."
                    
            formatted_blocks.append(b.replace("\n", "<br>"))

        formatted = "<br><br>".join(formatted_blocks)
        div = "<br>------------------------------------------------------------------------------------------------------------------<br>"
        return f"{div}<br>{formatted}<br>{div}"

    def _generate_email_content(self) -> str:
        """Generate HTML content to be sent by email based on
        search_report dictionary
        """
        import re

        current_directory = os.path.dirname(__file__)
        tm = TemplateManager(template_dir=os.path.join(current_directory, "templates"))
        
        # Consolidação dos resultados de todas as "Partes"
        consolidated_results = {}
        filters_content = {}
        main_header_title = self.report_config.header_text or ""
        no_result_message = self.report_config.no_results_found_text

        for search in self.search_report:
            # Pega o primeiro header title limpo se não tiver
            if not main_header_title and search["header"]:
                main_header_title = re.sub(r'\s*-\s*PARTE\s*\d+', '', search["header"])

            if not self.report_config.hide_filters and not filters_content:
                if search["department"] or search["department_ignore"] or search["pubtype"]:
                    filters_content = {"title": "Filtros Aplicados na Pesquisa:"}
                    if search["department"]:
                        filters_content["included_units"] = {
                            "title": "Unidades Incluídas:", "items": [f"{dpt}" for dpt in search["department"]]
                        }

            for group, search_results in search["result"].items():
                if not search_results: continue
                for term, term_results in search_results.items():
                    for department, results in term_results.items():
                        key = (group, term, department)
                        if key not in consolidated_results:
                            consolidated_results[key] = []
                        consolidated_results[key].extend(results)

        report_data = []
        if not consolidated_results:
            report_data.append({
                "search_terms": {
                    "department": "", "terms": [], "items": [{"header_title": main_header_title}]
                },
                "filters": filters_content,
                "header_title": main_header_title,
            })
        else:
            for (group, term, department), results in consolidated_results.items():
                term_data = {
                    "search_terms": {"department": "", "terms": [], "items": []},
                    "filters": filters_content,
                    "header_title": main_header_title,
                }
                if not self.report_config.hide_filters:
                    if group != "single_group": term_data["search_terms"]["group"] = group
                    if term != "all_publications": term_data["search_terms"]["terms"].append(f"{term}")
                    else: term_data["search_terms"]["terms"].append(f"{term}")

                if department != "single_department":
                    term_data["search_terms"]["department"] = f"{department}"

                for result in results:
                    title = result["title"] or "Documento sem título"
                    display_abstract = EmailSender.format_abstract(result.get("abstract", ""), cnpj_log=term)

                    ai_sufix = True if result.get("ai_generated") else None

                    term_data["search_terms"]["items"].append({
                        "section": result["section"],
                        "header_title": main_header_title,
                        "title": title,
                        "url": result["href"],
                        "url_new_tab": True,
                        "abstract": display_abstract,
                        "date": result["date"],
                        "ai_sufix": ai_sufix,
                        "has_ementa": result.get("has_ementa", False),
                        "full_text": result.get("full_text", False),
                    })
                report_data.append(term_data)

        return tm.renderizar(
            "dou_template.html",
            results=report_data,
            hide_filters=self.report_config.hide_filters,
            header_text=main_header_title,
            footer=self.report_config.footer_text or None,
            no_results_message=no_result_message,
        )

    def get_csv_tempfile(self) -> NamedTemporaryFile:
        temp_file = NamedTemporaryFile(prefix="extracao_dou_", suffix=".csv")
        self.convert_report_to_dataframe().to_csv(temp_file, index=False)
        return temp_file

    def convert_report_to_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame(self.convert_report_dict_to_tuple_list())
        df.columns = [
            "Consulta",
            "Grupo",
            "Termo de pesquisa",
            "Unidade",
            "Seção",
            "URL",
            "Título",
            "Resumo",
            "Data",
        ]
        del_header = True
        del_single_group = True
        del_single_department = True

        for search in self.search_report:
            if search["header"] is not None:
                del_header = False

            for group, search_result in search["result"].items():
                if group != "single_group":
                    del_single_group = False
                for _, term_results in search_result.items():
                    for dpt, _ in term_results.items():
                        if dpt != "single_department":
                            del_single_department = False

        # Drop empty or default columns
        if del_header:
            del df["Consulta"]

        if del_single_group:
            del df["Grupo"]
        else:
            # Replace single_group with blank
            df["Grupo"] = df["Grupo"].replace("single_group", "")

        if del_single_department:
            del df["Unidade"]
        else:
            # Replace single_group with blank
            df["Unidade"] = df["Unidade"].replace("single_department", "")

        return df

    def convert_report_dict_to_tuple_list(self) -> list:
        tuple_list = []
        for search in self.search_report:
            header = search["header"] if search["header"] else None
            for group, results in search["result"].items():
                for term, departments in results.items():
                    for department, dpt_matches in departments.items():
                        for match in dpt_matches:
                            tuple_list.append(
                                repack_match(header, group, term, department, match)
                            )
        return tuple_list


def repack_match(
    header: str, group: str, search_term: str, department: str, match: dict
) -> tuple:
    return (
        header,
        group,
        search_term,
        department,
        match["section"],
        match["href"],
        match["title"],
        match["abstract"],
        match["date"],
    )
