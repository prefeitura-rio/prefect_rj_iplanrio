"""Motor de processamento de templates HTML."""
from pathlib import Path
from jinja2 import Environment, FileSystemLoader, TemplateNotFound
from datetime import datetime


class TemplateEngine:
    """Processa templates HTML substituindo variáveis dinamicamente."""
    
    def __init__(self, template_path: str):
        """
        Inicializa o processador de templates.
        
        Args:
            template_path: Caminho para o arquivo de template HTML
        """
        self.template_path = Path(template_path)
        if not self.template_path.exists():
            raise FileNotFoundError(f"Template não encontrado: {template_path}")
        
        # Configura Jinja2 para carregar templates do diretório
        template_dir = self.template_path.parent
        self.env = Environment(
            loader=FileSystemLoader(str(template_dir)),
            autoescape=True  # Escapa HTML automaticamente para segurança
        )
        
        # Carrega o template
        template_name = self.template_path.name
        try:
            self.template = self.env.get_template(template_name)
        except TemplateNotFound:
            raise FileNotFoundError(f"Template não encontrado: {template_name}")
    
    def render(self, **variables) -> str:
        """
        Renderiza o template com as variáveis fornecidas.
        
        Args:
            **variables: Variáveis para substituir no template (ex: nome, data, etc.)
        
        Returns:
            HTML renderizado como string
        """
        # Adiciona data/hora atual se não fornecida
        if "data" not in variables:
            variables["data"] = datetime.now().strftime("%d/%m/%Y")
        if "hora" not in variables:
            variables["hora"] = datetime.now().strftime("%H:%M:%S")
        
        return self.template.render(**variables)

