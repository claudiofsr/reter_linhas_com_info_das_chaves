use clap::Parser;
use rand::Rng;
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

use crate::{COLUNAS_DOC, COLUNAS_EFD, REGEX_SEARCH_CSV, SpedError, SpedResult};

// Estrutura para o Clap processar os argumentos da linha de comando
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Arguments {
    /// Clear screen
    #[arg(short, long, default_value_t = false)]
    clear: bool,

    /// Imprimir chaves contidas em Documentos Fiscais
    #[arg(long, default_value_t = false)]
    docs_keys: bool,

    /// Imprimir chaves contidas na EFD Contribuições
    #[arg(long, default_value_t = false)]
    efd_keys: bool,

    /// Arquivo da EFD Contribuições.
    ///
    /// Arquivo esperado:
    ///
    /// - `Info do Contribuinte EFD Contribuicoes.csv`
    #[arg(short, long, required = true)]
    efd_path: Option<PathBuf>,

    /// Ativar modo detalhado (verbose)
    #[arg(short, long, default_value_t = false)]
    verbose: bool,
}

#[derive(Debug)]
pub struct Config {
    pub clear: bool,
    pub docs_keys: bool,
    pub efd_keys: bool,
    pub efd_path: PathBuf,
    pub verbose: bool,

    // Lista de arquivos de suporte (NFes/CTes)
    pub arquivos_csv: Vec<PathBuf>,

    pub target: PathBuf,
    // Referências para os HasMaps estáticos
    pub colunas_efd: &'static HashMap<&'static str, &'static str>,
    pub colunas_doc: &'static HashMap<&'static str, &'static str>,

    pub nfe_ctes: HashMap<String, HashSet<String>>,
    pub cte_nfes: HashMap<String, HashSet<String>>,
    pub cte_complementar: HashMap<String, HashSet<String>>,
    pub total_de_itens_analisados: usize,
}

impl Config {
    pub fn to_hash(&self, path: &Path) -> String {
        let hash = blake3::hash(path.display().to_string().as_bytes());
        format!("{}.tmp.{}", self.target.display(), hash)
    }
}

pub fn get_config() -> SpedResult<Config> {
    let args = Arguments::parse();

    // 1. Extração funcional: Converte Option<PathBuf> em PathBuf ou retorna erro
    // Como o Clap já exige 'required = true', este erro só ocorreria em casos extremos.
    let efd_path = args.efd_path.ok_or(SpedError::EfdFileNotFound)?;

    // 2. Buscar arquivos CSV de NFes/CTes no diretório atual.
    let arquivos_csv = search_csv_files(Path::new("."))?;

    // 3. Imprimir aqui (ou na main), mantendo a função de busca "pura"
    if !arquivos_csv.is_empty() {
        println!(" Arquivo(s) de NFe/CTe de formato CSV encontrado(s) no diretório atual:\n");
        arquivos_csv.iter().enumerate().for_each(|(i, path)| {
            println!("{:6}: {}", i + 1, path.display());
        });
        println!();
    }

    // 4. Geração do Target (Funcional)
    let mut rng = rand::rng();
    let file_name = format!(
        "ZZZ-{:06}-Info da Receita sobre o Contribuinte.csv",
        rng.random_range(0..999999)
    );

    Ok(Config {
        clear: args.clear,
        docs_keys: args.docs_keys,
        efd_keys: args.efd_keys,
        efd_path,
        verbose: args.verbose,
        arquivos_csv,
        target: PathBuf::from(&file_name),
        // Apenas atribuímos as referências estáticas
        colunas_efd: &COLUNAS_EFD,
        colunas_doc: &COLUNAS_DOC,
        nfe_ctes: HashMap::new(),
        cte_nfes: HashMap::new(),
        cte_complementar: HashMap::new(),
        total_de_itens_analisados: 0,
    })
}

/// Procura arquivos CSV no diretório atual baseando-se nos padrões do ReceitaNet-BX.
pub fn search_csv_files(dir: &std::path::Path) -> SpedResult<Vec<PathBuf>> {
    // 1. Leitura funcional do diretório
    let mut arquivos_csv: Vec<PathBuf> = fs::read_dir(dir)
        .map_err(SpedError::Io)?
        .flatten() // Transforma Result<DirEntry> em DirEntry, ignorando erros individuais
        .filter_map(|entry| {
            let path = entry.path();
            let is_match = path
                .file_name()
                .and_then(|n| n.to_str())
                .map(|name| REGEX_SEARCH_CSV.is_match(name))
                .unwrap_or_default();

            if path.is_file() && is_match {
                Some(path)
            } else {
                None
            }
        })
        .collect();

    // 2. Validação de existência
    if arquivos_csv.is_empty() {
        return Err(SpedError::NoCSVFilesFound);
    }

    // 3. Ordenação (alfabética)
    arquivos_csv.sort();

    Ok(arquivos_csv)
}
