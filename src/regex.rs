use regex::Regex;
use std::sync::LazyLock;

/// Regex consolidada seguindo o padrão (?isx)
/// i: case-insensitive
/// s: '.' inclui \n (embora nomes de arquivos raramente tenham \n)
/// x: modo verbose (ignora espaços e permite comentários)
pub static REGEX_SEARCH_CSV: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?isx)
        ^ # Início da string
        (?:
            \d{4}.*CTe.*Destinatario   | # Ex: 2023-CTe-Destinatario
            \d{4}.*CTe.*Remetente      | # Ex: 2023-CTe-Remetente
            \d{4}.*NFe.*Destinatario   | # Ex: 2023-NFe-Destinatario
            \d{4}.*NFe.*Emitente       | # Ex: 2023-NFe-Emitente
            DadosAdicionais.*CTe       | # Ex: DadosAdicionais-CTe
            DadosAdicionais.*NFe         # Ex: DadosAdicionais-NFe
        )
        .*\.csv # Qualquer coisa seguida da extensão .csv
        $ # Fim da string
        ",
    )
    .unwrap()
});

// Regex para limpeza e validação
pub static RE_MULTISPACE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\s{2,}").unwrap());
pub static RE_NON_DIGITS: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\D").unwrap());
pub static RE_CHAVE_44: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(\d{44})$").unwrap());
