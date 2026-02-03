use std::{io, path::PathBuf};
use thiserror::Error;

/// Tipo de retorno conveniente para todo o projeto
pub type SpedResult<T> = Result<T, SpedError>;

#[derive(Error, Debug)]
pub enum SpedError {
    #[error(
        "Erro no número de colunas!\n\
        Arquivo: {arquivo:?}\n\
        Linha nº: {linha}\n\
        Esperado: {esperado} colunas\n\
        Encontrado: {encontrado} colunas"
    )]
    ColumnCount {
        arquivo: PathBuf,
        linha: usize,
        esperado: usize,
        encontrado: usize,
    },

    #[error("Erro de configuração: {0}")]
    Config(String),

    // Adicione esta linha:
    #[error("Erro no processamento CSV: {0}")]
    Csv(#[from] csv::Error),

    #[error("Erro na linha {linha}: esperado {esperado} colunas, encontrado {encontrado}")]
    CsvColumnMismatch {
        linha: usize,
        esperado: usize,
        encontrado: usize,
    },

    #[error("Arquivo <{arquivo}> contém colunas repetidas: <{coluna}> no arquivo <{arquivo}>")]
    DuplicateColumnName { arquivo: PathBuf, coluna: String },

    #[error(
        "Arquivo EFD não definido ou inválido!\n\
        Exemplo:\n\
        reter_linhas_com_info_das_chaves -n 15 -e 'Info do Contribuinte EFD Contribuicoes.csv'"
    )]
    EfdFileNotFound,

    #[error("Arquivo <{arquivo}> contém colunas com nome em branco!")]
    EmptyColumnName { arquivo: PathBuf },

    #[error("CNPJ inválido: {cnpj}. Esperado 14 dígitos, encontrado {length}")]
    InvalidCnpj { cnpj: String, length: usize },

    #[error("Erro de I/O: {0}")]
    Io(#[from] io::Error),

    #[error(
        "Arquivo EFD não encontrado!\n\
        Arquivo: {arquivo:?}\n\
        {source}"
    )]
    IoReader {
        #[source] // Indica que este é o erro original
        source: io::Error,
        arquivo: PathBuf,
    },

    #[error("Coluna essencial ausente no arquivo <{arquivo}>: {coluna} (Tipo: {tipo:?})")]
    MissingEssentialColumn {
        arquivo: PathBuf,
        coluna: String,
        tipo: crate::sped_efd::TipoDeArquivo,
    },

    #[error("NFes/CTes CSV files not found in directory!")]
    NoCSVFilesFound,

    #[error("Falha ao processar arquivo paralelo: {0}")]
    ParallelProcessing(String),

    #[error("Regex Error: {0}")]
    Regex(#[from] regex::Error),
}

impl SpedError {
    pub fn from_csv(e: csv::Error, arquivo: PathBuf, linha: usize) -> Self {
        // Ajustado para usar os campos 'expected_len' e 'len' conforme a definição do ErrorKind
        if let csv::ErrorKind::UnequalLengths {
            expected_len, len, ..
        } = e.kind()
        {
            return SpedError::ColumnCount {
                arquivo,
                linha,
                esperado: *expected_len as usize,
                encontrado: *len as usize,
            };
        }
        // Caso seja qualquer outro erro (IO, UTF8, etc), converte para SpedError::Csv
        SpedError::Csv(e) // Ou sua conversão padrão
    }
}
