from pipelines.rj_crm__modelo_qualidade_telefone.constants import QUERIES_ANCHOR
from pipelines.rj_crm__modelo_qualidade_telefone.helpers.features import features_sql
from utils.utils import render_query


def test_feature_queries_read_the_flat_v2_conversation_layout() -> None:
    """Read message fields from v2 without the removed message array."""
    queries = [
        features_sql("now"),
        render_query(QUERIES_ANCHOR, "eventos_candidatos", janela_recente_dias=30),
    ]

    for sql in queries:
        assert "rmi_conversas.v2_chatbot_conversas" in sql
        assert "c.mensagem_datahora" in sql
        assert "c.mensagens[" not in sql
        assert "UNNEST(c.mensagens)" not in sql
