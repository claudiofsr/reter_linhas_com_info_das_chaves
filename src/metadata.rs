use std::collections::HashMap;
use std::sync::LazyLock;

// --- Tabelas de Referência ---

/// Modelos de Documentos Fiscais - Tabela 4.1.1
/// Otimizado para não usar memória RAM (armazenado no binário)
pub fn get_modelo_documentos_fiscais(codigo: &str) -> &'static str {
    match codigo {
        "01" => "Nota Fiscal",
        "1B" => "Nota Fiscal Avulsa",
        "02" => "Nota Fiscal de Venda a Consumidor",
        "2D" => "Cupom Fiscal emitido por ECF",
        "2E" => "Bilhete de Passagem emitido por ECF",
        "04" => "Nota Fiscal de Produtor",
        "06" => "Nota Fiscal / Conta de Energia Elétrica",
        "07" => "Nota Fiscal de Serviço de Transporte",
        "08" => "Conhecimento de Transporte Rodoviário de Cargas",
        "8B" => "Conhecimento de Transporte de Cargas Avulso",
        "09" => "Conhecimento de Transporte Aquaviário de Cargas",
        "10" => "Conhecimento Aéreo",
        "11" => "Conhecimento de Transporte Ferroviário de Cargas",
        "13" => "Bilhete de Passagem Rodoviário",
        "14" => "Bilhete de Passagem Aquaviário",
        "15" => "Bilhete de Passagem e Nota de Bagagem",
        "16" => "Bilhete de Passagem Ferroviário",
        "17" => "Despacho de Transporte",
        "18" => "Resumo de Movimento Diário",
        "20" => "Ordem de Coleta de Cargas",
        "21" => "Nota Fiscal de Serviço de Comunicação",
        "22" => "Nota Fiscal de Serviço de Telecomunicação",
        "23" => "GNRE",
        "24" => "Autorização de Carregamento e Transporte",
        "25" => "Manifesto de Carga",
        "26" => "Conhecimento de Transporte Multimodal de Cargas",
        "27" => "Nota Fiscal de Transporte Ferroviário de Cargas",
        "28" => "Nota Fiscal / Conta de Fornecimento de Gás Canalizado",
        "29" => "Nota Fiscal / Conta de Fornecimento de Água Canalizada",
        "30" => "Bilhete / Recibo do Passageiro",
        "55" => "Nota Fiscal Eletrônica: NF-e",
        "57" => "Conhecimento de Transporte Eletrônico: CT-e",
        "59" => "Cupom Fiscal Eletrônico: CF-e (CF-e-SAT)",
        "60" => "Cupom Fiscal Eletrônico: CF-e-ECF",
        "63" => "Bilhete de Passagem Eletrônico: BP-e",
        "65" => "Nota Fiscal Eletrônica ao Consumidor Final: NFC-e",
        "66" => "Nota Fiscal de Energia Elétrica Eletrônica: NF3e",
        "67" => "Conhecimento de Transporte Eletrônico para Outros Serviços: CT-e OS",
        _ => "Modelo Desconhecido",
    }
}

// Mapeamento estático para colunas EFD
pub static COLUNAS_EFD: LazyLock<HashMap<&'static str, &'static str>> = LazyLock::new(|| {
    HashMap::from([
        ("num_linha", "Linhas"),
        ("efd_file", "Arquivo da EFD Contribuições"),
        ("efd_line", "Nº da Linha da EFD"),
        (
            "cnpj_contribuinte",
            "CNPJ dos Estabelecimentos do Contribuinte",
        ),
        ("nome_contribuinte", "Nome do Contribuinte"),
        ("periodo_apuracao", "Período de Apuração"),
        ("periodo_apuracao_ano", "Ano do Período de Apuração"),
        ("periodo_apuracao_tri", "Trimestre do Período de Apuração"),
        ("periodo_apuracao_mes", "Mês do Período de Apuração"),
        ("tipo_de_operacao", "Tipo de Operação"),
        ("tipo_de_credito", "Tipo de Crédito"),
        ("registro_bloco", "Registro"),
        ("codigo_cst", "Código de Situação Tributária (CST)"),
        (
            "codigo_cfop",
            "Código Fiscal de Operações e Prestações (CFOP)",
        ),
        ("natureza_bc", "Natureza da Base de Cálculo dos Créditos"),
        ("cnpj_participante", "CNPJ do Participante"),
        ("cpf_participante", "CPF do Participante"),
        ("nome_participante", "Nome do Participante"),
        ("num_doc_fiscal", "Nº do Documento Fiscal"),
        ("chave_documento", "Chave do Documento"),
        ("modelo_doc_fiscal", "Modelo do Documento Fiscal"),
        ("num_item", "Nº do Item do Documento Fiscal"),
        ("data_emissao_nota", "Data da Emissão do Documento Fiscal"),
        (
            "data_lancamento",
            "Data da Entrada / Aquisição / Execução ou da Saída / Prestação / Conclusão",
        ),
        ("tipo_do_item", "Tipo do Item"),
        ("descricao_do_item", "Descrição do Item"),
        ("codigo_ncm", "Código NCM"),
        (
            "escrituracao_contabil",
            "Escrituração Contábil: Nome da Conta",
        ),
        (
            "info_complem_doc_fiscal",
            "Informação Complementar do Documento Fiscal",
        ),
        ("valor_do_item", "Valor Total do Item"),
        (
            "valor_bc_contrib",
            "Valor da Base de Cálculo das Contribuições",
        ),
        ("aliq_pis", "Alíquota de PIS/PASEP (em percentual)"),
        ("aliq_cofins", "Alíquota de COFINS (em percentual)"),
        ("valor_de_pis", "Valor de PIS/PASEP"),
        ("valor_de_cofins", "Valor de COFINS"),
        ("valor_de_iss", "Valor de ISS"),
        ("valor_bc_icms", "Valor da Base de Cálculo de ICMS"),
        ("aliq_icms", "Alíquota de ICMS (em percentual)"),
        ("valor_de_icms", "Valor de ICMS"),
    ])
});

// Mapeamento estático para colunas DOC
pub static COLUNAS_DOC: LazyLock<HashMap<&'static str, &'static str>> = LazyLock::new(|| {
    HashMap::from([
        (
            "cnpj_contribuinte",
            "CNPJ do Contribuinte : NF Item (Todos)",
        ),
        (
            "nome_contribuinte",
            "Nome do Contribuinte : NF Item (Todos)",
        ),
        ("entrada_ou_saida", "Entrada/Saída : NF (Todos)"),
        ("cnpj_participante", "CPF/CNPJ do Participante : NF (Todos)"),
        ("nome_participante", "Nome do Participante : NF (Todos)"),
        ("codigo_crt", "CRT : NF (Todos)"),
        ("observacoes", "Observações : NF (Todos)"),
        (
            "cnpj_do_remetente_ind01",
            "CTe - Remetente das mercadorias transportadas: CNPJ/CPF de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes",
        ),
        (
            "cnpj_do_remetente_ind02",
            "CTe - Remetente das mercadorias transportadas: CNPJ/CPF de Conhecimento : ConhecimentoInformacaoNFe",
        ),
        (
            "remetente_nome",
            "CTe - Remetente das mercadorias transportadas: Nome de Conhecimento : ConhecimentoInformacaoNFe",
        ),
        (
            "remetente_municipio",
            "CTe - Remetente das mercadorias transportadas: Município de Conhecimento : ConhecimentoInformacaoNFe",
        ),
        (
            "papel_tomador_ind01",
            "Descrição CTe - Indicador do 'papel' do tomador do serviço de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes",
        ),
        (
            "papel_tomador_ind02",
            "Descrição CTe - Indicador do 'papel' do tomador do serviço de Conhecimento : ConhecimentoInformacaoNFe",
        ),
        (
            "cnpj_do_tomador_ind01",
            "CTe - Outro tipo de Tomador: CNPJ/CPF de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes",
        ),
        (
            "cnpj_do_tomador_ind02",
            "CTe - Outro tipo de Tomador: CNPJ/CPF de Conhecimento : ConhecimentoInformacaoNFe",
        ),
        (
            "inicio_estado",
            "CTe - UF do início da prestação de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes",
        ),
        (
            "inicio_munici",
            "CTe - Nome do Município do início da prestação de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes",
        ),
        (
            "final_estado",
            "CTe - UF do término da prestação de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes",
        ),
        (
            "final_munici",
            "CTe - Nome do Município do término da prestação de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes",
        ),
        (
            "info_destinatario_cnpj",
            "CTe - Informações do Destinatário do CT-e: CNPJ/CPF de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes",
        ),
        (
            "info_destinatario_nome",
            "CTe - Informações do Destinatário do CT-e: Nome de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes",
        ),
        (
            "local_entrega",
            "CTe - Local de Entrega constante na Nota Fiscal: Nome de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes",
        ),
        (
            "descricao_nat_oper",
            "Descrição da Natureza da Operação : NF Item (Todos)",
        ),
        ("nota_cancelada", "Cancelada : NF (Todos)"),
        (
            "registro_de_origem",
            "Registro de Origem do Item : NF Item (Todos)",
        ),
        (
            "natureza_bc",
            "Natureza da Base de Cálculo do Crédito Descrição : NF Item (Todos)",
        ),
        ("modelo_descricao", "Modelo - Descrição : NF Item (Todos)"),
        ("num_doc_fiscal", "Número da Nota : NF Item (Todos)"),
        (
            "chave44_digitos",
            "Chave da Nota Fiscal Eletrônica : NF Item (Todos)",
        ),
        (
            "chave_de_acesso",
            "Inf. NFe - Chave de acesso da NF-e : ConhecimentoInformacaoNFe",
        ),
        (
            "observacoes_gerais",
            "CTe - Observações Gerais de Conhecimento : ConhecimentoInformacaoNFe",
        ),
        ("dia_emissao_nota", "Dia da Emissão : NF Item (Todos)"),
        ("num_di", "Número da DI : NF Item (Todos)"),
        ("num_item", "Número do Item : NF Item (Todos)"),
        ("codigo_cfop", "Código CFOP : NF Item (Todos)"),
        ("descricao_cfop", "Descrição CFOP : NF Item (Todos)"),
        (
            "descricao_da_mercadoria",
            "Descrição da Mercadoria/Serviço : NF Item (Todos)",
        ),
        ("codigo_ncm", "Código NCM : NF Item (Todos)"),
        ("descricao_ncm", "Descrição NCM : NF Item (Todos)"),
        (
            "aliquota_cofins",
            "COFINS: Alíquota ad valorem - Atributo : NF Item (Todos)",
        ),
        (
            "aliquota_pis",
            "PIS: Alíquota ad valorem - Atributo : NF Item (Todos)",
        ),
        ("cst_cofins_descr", "CST COFINS Descrição : NF Item (Todos)"),
        ("cst_pis_descr", "CST PIS Descrição : NF Item (Todos)"),
        ("valor_total", "Valor Total : NF (Todos) SOMA"),
        (
            "valor_proporcional",
            "Valor da Nota Proporcional : NF Item (Todos) SOMA",
        ),
        (
            "valor_descontos",
            "Valor dos Descontos : NF Item (Todos) SOMA",
        ),
        ("valor_seguro", "Valor Seguro : NF (Todos) SOMA"),
        (
            "valor_tributo_cofins",
            "COFINS: Valor do Tributo : NF Item (Todos) SOMA",
        ),
        (
            "valor_tributo_pis",
            "PIS: Valor do Tributo : NF Item (Todos) SOMA",
        ),
        (
            "valor_tributo_ipi",
            "IPI: Valor do Tributo : NF Item (Todos) SOMA",
        ),
        (
            "valor_bc_iss",
            "ISS: Base de Cálculo : NF Item (Todos) SOMA",
        ),
        (
            "valor_tributo_iss",
            "ISS: Valor do Tributo : NF Item (Todos) SOMA",
        ),
        ("aliquota_icms", "ICMS: Alíquota : NF Item (Todos) NOISE OR"),
        (
            "valor_bc_icms",
            "ICMS: Base de Cálculo : NF Item (Todos) SOMA",
        ),
        (
            "valor_icms",
            "ICMS: Valor do Tributo : NF Item (Todos) SOMA",
        ),
        (
            "valor_icms_sub",
            "ICMS por Substituição: Valor do Tributo : NF Item (Todos) SOMA",
        ),
    ])
});
