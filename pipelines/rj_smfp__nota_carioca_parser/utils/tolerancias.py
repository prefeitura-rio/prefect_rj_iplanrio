"""Conjunto de regras de tolerância por versão, na ordem em que o .NET as registra (``GetValidationHandler``)."""

from pipelines.rj_smfp__nota_carioca_parser.utils import tolerancias_estrutura as est
from pipelines.rj_smfp__nota_carioca_parser.utils import tolerancias_valores as val
from pipelines.rj_smfp__nota_carioca_parser.utils import tolerancias_versoes as ver
from pipelines.rj_smfp__nota_carioca_parser.utils.erros_xsd import ErroXsd
from pipelines.rj_smfp__nota_carioca_parser.utils.tolerancias_base import Regra
from pipelines.rj_smfp__nota_carioca_parser.utils.versao import Versao

REGRAS_COMUNS: tuple[Regra, ...] = (
    val.tolerar_xnbs_texto,
    val.tolerar_xtribmun_texto,
    val.tolerar_cep_curto,
    val.tolerar_fone_fora_da_faixa,
    val.tolerar_dtfim_invalida,
    val.tolerar_dtini_invalida,
    val.tolerar_id_pedido_evento,
    val.tolerar_clocincid_invalido,
    val.tolerar_xbairro_vazio,
    val.tolerar_xdescserv_vazio,
    val.tolerar_versao_fora_do_padrao,
    val.tolerar_im_vazia,
    val.tolerar_cendpost_vazio,
    val.tolerar_xnome_vazio,
    val.tolerar_padrao_xlgr,
    val.tolerar_padrao_nro,
    val.tolerar_padrao_xcpl,
    val.tolerar_padrao_xbairro,
    val.tolerar_padrao_xnome,
    val.tolerar_padrao_xfant,
    val.tolerar_padrao_email,
    val.tolerar_valor_monetario_fora_do_padrao,
    val.tolerar_xsi_nil_indevido,
    val.tolerar_serie_dps_ancorada,
    est.tolerar_paliq_fora_de_ordem,
    est.tolerar_xbairro_antes_de_nro,
    est.tolerar_cmun_antes_de_complemento,
    est.tolerar_cpaisprestacao_em_locprest,
    est.tolerar_locprest_incompleto,
    est.tolerar_obra_incompleta,
    est.tolerar_cobra_em_obra,
    est.tolerar_end_em_obra,
    est.tolerar_tribnac_antes_de_tribfed,
    est.tolerar_xoutinf_fora_de_ordem,
    est.tolerar_algoritmo_fora_da_enumeracao,
    est.tolerar_algoritmo_diferente_do_fixo,
    est.tolerar_transforms_sem_transform,
    est.tolerar_uri_vazia_em_reference,
    est.tolerar_nbm_antes_de_tpbm,
    est.tolerar_transform_invalido_em_transforms,
    est.tolerar_reference_invalida_em_signedinfo,
    est.tolerar_x509subjectname_em_x509data,
    est.tolerar_object_em_signature,
    est.tolerar_nt007_novos_elementos_em_ibscbs,
    est.tolerar_nt007_indfinal_ausente,
    est.tolerar_nt007_gdedredibscbs,
    est.tolerar_nt007_gestornocred_gpagantecipado,
    est.tolerar_nt007_vcalcdedredibscbs,
    est.tolerar_nt007_xped,
    est.tolerar_nt007_gitemped,
    est.tolerar_nt007_ccib_em_obra,
    est.tolerar_uf_vazio,
    est.tolerar_mun_vazio,
    est.tolerar_fed_vazio,
    est.tolerar_uf_sem_aliquota_efetiva,
    est.tolerar_mun_sem_aliquota_efetiva,
    est.tolerar_fed_sem_aliquota_efetiva,
)

REGRAS_V100: tuple[Regra, ...] = (
    ver.tolerar_cstat_107,
    ver.tolerar_ndfe_legado_em_infevento,
    ver.tolerar_xnome_em_atvevento,
    ver.tolerar_idatvevt_em_atvevento,
    ver.tolerar_cmotivoemisti_em_infdps,
    ver.tolerar_chnfserej_em_infdps,
    ver.tolerar_npedregevento_em_infpedreg,
    ver.tolerar_cep_em_endereco_simples,
    ver.tolerar_endext_em_endereco_simples,
    ver.tolerar_inscimobfisc_em_obra,
    ver.tolerar_tpbm_valor_antigo,
    ver.tolerar_xdesc_valor_antigo,
    ver.tolerar_inforej_legado,
    ver.tolerar_anulacao_inline_te205208,
    ver.tolerar_ordem_em_e305102,
    ver.tolerar_ibscbs_em_infnfse,
    ver.tolerar_id_pedido_formato_curto,
)

REGRAS_V101: tuple[Regra, ...] = (
    ver.tolerar_ndfe_legado_em_infevento,
    ver.tolerar_npedregevento_em_infpedreg,
    ver.tolerar_inforej_legado,
    ver.tolerar_xdesc_valor_antigo,
    ver.tolerar_cstat_101,
    ver.tolerar_eventos_907202_967203,
    ver.tolerar_id_pedido_formato_antigo,
    ver.tolerar_id_evento_formato_antigo,
)

VERSAO_1_00 = Versao(1, 0)
VERSAO_1_01 = Versao(1, 1)


def regras_para(versao: Versao) -> tuple[Regra, ...]:
    """Regras da versão: as comuns, mais as da v1.00 (até 1.0) e as da v1.01 (a partir de 1.1)."""
    regras = REGRAS_COMUNS
    if versao <= VERSAO_1_00:
        regras += REGRAS_V100
    if versao >= VERSAO_1_01:
        regras += REGRAS_V101
    return regras


def tolera(erro: ErroXsd, regras: tuple[Regra, ...]) -> bool:
    """Verdadeiro se alguma regra ignora o erro."""
    return any(regra(erro) for regra in regras)
