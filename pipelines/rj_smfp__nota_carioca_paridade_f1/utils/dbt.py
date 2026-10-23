"""Parâmetros do run de dbt de paridade, disparado pelo deployment compartilhado ``rj-iplanrio--run_dbt--prod``."""

import json

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.constants import default_dbt_parameters


def build_dbt_parameters(select: str, corte_nsu: int | None) -> dict[str, object]:
    """Parâmetros do flow ``rj_iplanrio__run_dbt`` para o ``dbt build`` da paridade.

    O flow de dbt não tem parâmetro ``vars``: a única porta é ``flag``, que ele entrega ao dbt como um argumento só.
    Por isso as variáveis vão em ``--vars=<json>`` (uma única palavra).

    :param select: Seleção do dbt (``tag:paridade_nc2``).
    :param corte_nsu: Corte de NSU (``nc2_corte_nsu``); ``None`` deixa o dbt calcular o seu.
    :returns: Parâmetros do deployment de dbt.
    """
    variables: dict[str, object] = {"nota_carioca_v2_habilitado": True}
    if corte_nsu is not None:
        variables["nc2_corte_nsu"] = corte_nsu
    parameters = default_dbt_parameters()
    parameters.update({"command": "build", "select": select, "flag": f"--vars={json.dumps(variables)}"})
    return parameters
