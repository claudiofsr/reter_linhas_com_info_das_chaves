use rayon::prelude::*;
use regex::Regex;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt::Display,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicUsize, Ordering},
};

use crate::{
    Config, RE_CHAVE_44, RE_MULTISPACE, RE_NON_DIGITS, SpedError, SpedResult,
    get_modelo_documentos_fiscais,
};

/// Limpar a tela.
pub fn clear_screen(clear_screen: bool) -> SpedResult<()> {
    if clear_screen {
        if cfg!(target_os = "windows") {
            // No Windows, 'cls' é um comando interno do 'cmd'.
            // Precisamos chamar o interpretador para executá-lo.
            Command::new("cmd").args(["/c", "cls"]).status()?;
        } else {
            // Em Linux/macOS, o comando 'clear' costuma ser um executável independente.
            Command::new("clear").status()?;
        }
    }

    Ok(())
}

/// Exibe a descrição, autoria e versão do programa.
/// Equivalente a Imprimir_Versao_do_Programa em Perl.
pub fn imprimir_versao_do_programa() {
    let descr = [
        "Este programa analisa informações do arquivo de EFD Contribuições e de arquivos de Documentos Fiscais.",
        "Estas informações são identificadas por chaves numéricas (cada chave pode conter vários itens).",
        "As chaves são compostas por 44 dígitos e classificadas por códigos de 2 dígitos internos.",
        "As chaves classificadas como NFe possuem o código 55.",
        "As chaves classificadas como CTe possuem o código 57.",
        "Do arquivo de EFD Contribuições são retidas as informações das chaves NFes e CTes.",
        "Em seguida, informações destas chaves indicadas no arquivo de EFD Contribuições são pesquisadas nos arquivos de Documentos Fiscais.",
        "As informações das chaves encontradas nos arquivos de Documentos Fiscais são retidas no arquivo <Info da Receita sobre o Contribuinte.csv>.",
        "As informações das chaves não encontradas nos arquivos de Documentos Fiscais são indicadas em arquivos com códigos de 44 dígitos a serem pesquisados no ReceitaNet-BX.",
    ];

    let author = "Claudio Fernandes de Souza Rodrigues (claudiofsr@yahoo.com)";
    let date = "7 de Janeiro de 2026 (inicio: 01 de Outubro de 2018)";
    let version = "0.80";

    // Loop de impressão da descrição (semelhante ao foreach do Perl)
    for line in &descr {
        println!(" {}", line);
    }

    // Impressão do rodapé utilizando interpolação de strings
    println!("\n {}\n {}\n versão: {}\n", author, date, version);
}

/// Tipo alias para representar o mapa de relações entre chaves de CTe.
pub type KeyMap = HashMap<String, HashSet<String>>;

/// Verifica se a chave tem 44 caracteres e se o modelo (posições 21-22) coincide.
fn eh_modelo(chave: &str, modelo: &str) -> bool {
    chave.len() == 44 && chave.get(20..22) == Some(modelo)
}

pub fn ler_todas_as_nfes_deste_cte<P>(path: P) -> SpedResult<KeyMap>
where
    P: AsRef<Path> + Clone + Display,
{
    let file = File::open(&path).map_err(|e| SpedError::IoReader {
        source: e,
        arquivo: path.clone().as_ref().to_path_buf(),
    })?;

    let reader = BufReader::new(file);

    // Compila o regex apenas uma vez.
    // \b garante que pegamos apenas sequências de 44 dígitos isoladas.
    let re = Regex::new(r"\b\d{44}\b")?;

    let hash: KeyMap = reader
        .lines()
        .par_bridge() // Transforma o iterador sequencial em paralelo
        .map(
            |line_result| -> SpedResult<Option<(String, HashSet<String>)>> {
                // Se houver erro de leitura na linha, o '?' propaga o SpedError::Io
                let line = line_result?;
                let mut chaves = re.find_iter(&line).map(|m| m.as_str());

                // O primeiro match deve ser o CT-e (modelo 57)
                match chaves.next() {
                    Some(cte) if eh_modelo(cte, "57") => {
                        // Os demais matches são as NFes (modelo 55)
                        let nfes: HashSet<String> = chaves
                            .filter(|nfe| eh_modelo(nfe, "55"))
                            .map(String::from)
                            .collect();

                        if nfes.is_empty() {
                            Ok(None)
                        } else {
                            Ok(Some((cte.to_string(), nfes)))
                        }
                    }
                    _ => Ok(None), // Não é uma linha de CT-e válida
                }
            },
        )
        // O truque mágico: transforma Result<Option<T>, E> em Option<Result<T, E>>
        // O filter_map remove os Nones, mas mantém os Errs
        .filter_map(|res| res.transpose())
        // O collect em um Result<Map, Error> interrompe no primeiro erro encontrado
        .collect::<SpedResult<KeyMap>>()?;

    // Estatísticas usando funcional
    let num_cte = hash.len();
    let num_nfe = hash.values().map(|v| v.len()).sum::<usize>();

    println!(
        "Encontrado {:>6} CTes contendo no total {:>6} NFes no arquivo <{}>.",
        fmt_milhares(num_cte),
        fmt_milhares(num_nfe),
        path
    );

    Ok(hash)
}

pub fn ler_chave_complementar_deste_cte<P>(path: P) -> SpedResult<KeyMap>
where
    P: AsRef<Path> + Clone + Display,
{
    let file = File::open(&path).map_err(|e| SpedError::IoReader {
        source: e,
        arquivo: path.clone().as_ref().to_path_buf(),
    })?;

    let reader = BufReader::new(file);
    let re = Regex::new(r"\b\d{44}\b")?;

    // No Rayon, try_fold e try_reduce trabalham juntos para processar e mesclar resultados
    let hash: KeyMap = reader
        .lines()
        .par_bridge() // Transforma o iterador sequencial em paralelo
        .try_fold(
            HashMap::new, // Criador de acumulador local para cada thread
            |mut acc: KeyMap, line_result| -> SpedResult<KeyMap> {
                let line = line_result?;
                let mut matches = re.find_iter(&line).map(|m| m.as_str());

                if let (Some(cte), Some(comp)) = (matches.next(), matches.next()) {
                    // Validação: Ambos modelo 57 e chaves diferentes
                    if eh_modelo(cte, "57") && eh_modelo(comp, "57") && cte != comp {
                        // Inserção bidirecional
                        acc.entry(cte.to_string())
                            .or_default()
                            .insert(comp.to_string());
                        acc.entry(comp.to_string())
                            .or_default()
                            .insert(cte.to_string());
                    }
                }
                Ok(acc)
            },
        )
        // try_reduce mescla os mapas parciais gerados pelas threads
        .try_reduce(HashMap::new, |mut map_a, map_b| {
            for (key, values) in map_b {
                map_a.entry(key).or_default().extend(values);
            }
            Ok(map_a)
        })?;

    let num_cte = hash.len();
    let num_com = hash.values().map(|v| v.len()).sum::<usize>();

    println!(
        "Encontrado {:>6} CTes contendo no total {:>6} CTes Complementares no arquivo <{}>.",
        fmt_milhares(num_cte),
        fmt_milhares(num_com),
        path
    );

    Ok(hash)
}

pub fn get_nfe_ctes(cte_nfes: &KeyMap) -> KeyMap {
    let mut nfe_ctes: KeyMap = HashMap::new();
    for (cte, nfes) in cte_nfes {
        for nfe in nfes {
            nfe_ctes.entry(nfe.clone()).or_default().insert(cte.clone());
        }
    }
    nfe_ctes
}

/// Expande as relações de transitividade entre CTes Complementares.
///
/// Esta função resolve o problema de encontrar "componentes conectados" em um grafo de documentos.
/// Se um CTe **A** referencia **B**, e **B** referencia **C**, a função entende que todos
/// pertencem ao mesmo grupo e atualiza o mapa para que todos apontem para todos.
///
/// ### Lógica de Negócio (Transitividade)
/// Em termos práticos, se houver uma cadeia de complementos (A -> B -> C), o algoritmo
/// garante que o resultado final contenha:
/// - A conhece {B, C}
/// - B conhece {A, C}
/// - C conhece {A, B}
///
/// ### Algoritmo
/// O processo é realizado em três etapas principais:
/// 1. **Simetrização**: Garante que se A aponta para B, B também aponte para A no grafo inicial.
/// 2. **Busca de Componentes**: Utiliza uma Busca em Profundidade (DFS) para agrupar todos os
///    CTes que possuem qualquer ligação entre si (direta ou indireta).
/// 3. **Clique (Expansão Total)**: Para cada grupo encontrado, reconstrói o mapa original
///    onde cada membro do grupo possui como vizinhos todos os outros integrantes.
///
/// ### Performance
/// Esta implementação utiliza a identificação de componentes conectados,
/// resultando em uma complexidade **O(V + E)**, onde:
/// - **V** é o número de chaves (vértices).
/// - **E** é o número de relações (arestas).
///
/// ### Exemplo
/// ```
/// use reter_linhas_com_info_das_chaves::{expand_cte_complementar, KeyMap};
/// use std::collections::HashMap;
///
/// let mut mapa: KeyMap = HashMap::new();
/// mapa.entry("A".to_string()).or_default().insert("B".to_string());
/// mapa.entry("B".to_string()).or_default().insert("C".to_string());
///
/// expand_cte_complementar(&mut mapa);
///
/// assert!(mapa.get("A").unwrap().contains("C"));
/// assert!(mapa.get("C").unwrap().contains("A"));
/// ```
pub fn expand_cte_complementar(map: &mut KeyMap) {
    // 1. Criar um grafo de adjacência simétrico para garantir bidirecionalidade
    let mut adj: HashMap<String, HashSet<String>> = HashMap::new();
    for (u, neighbors) in map.drain() {
        for v in neighbors {
            adj.entry(u.clone()).or_default().insert(v.clone());
            adj.entry(v).or_default().insert(u.clone());
        }
    }

    let mut visited = HashSet::new();
    let keys: Vec<String> = adj.keys().cloned().collect();

    for node in keys {
        if visited.contains(&node) {
            continue;
        }

        // 2. Identificar todos os membros da "ilha" (componente conectado) via DFS
        let mut group = Vec::new();
        let mut stack = vec![node];

        while let Some(current) = stack.pop() {
            if visited.insert(current.clone()) {
                group.push(current.clone());
                if let Some(neighbors) = adj.get(&current) {
                    stack.extend(neighbors.iter().cloned());
                }
            }
        }

        // 3. Criar a relação "todos com todos" (clique) para este grupo
        for member in &group {
            let mut others: HashSet<String> = group.iter().cloned().collect();
            others.remove(member); // Um CTe não é complementar de si mesmo

            if !others.is_empty() {
                map.insert(member.clone(), others);
            }
        }
    }
}

/// Expande a associação de NFEs para CTes complementares.
///
/// ### Lógica de Negócio
/// No transporte de cargas (SPED), um CTe Complementar herda as notas fiscais (NFEs)
/// do seu CTe de referência. Esta função garante que se o **CTe A** possui as
/// **Notas 1 e 2**, e o **CTe B** é complementar de **A**, então **B** também
/// passará a listar as **Notas 1 e 2**.
///
/// ### Otimização de Performance
/// Diferente da abordagem com `Vec<(String, String)>`, esta versão:
/// 1. Usa um `HashMap<String, HashSet<String>>` temporário para agrupar notas por CTe.
/// 2. Reduz a pressão sobre o alocador de memória ao evitar a criação de milhões de tuplas.
/// 3. Utiliza `extend` para mesclar conjuntos de dados de uma só vez, o que é mais
///    eficiente em Rust do que inserções individuais em loops.
///
/// Se um CTe de origem existe em ambos os mapas,
/// todos os seus "alvos" complementares recebem todas as suas NFEs.
///
/// ### Exemplo
/// ```
/// // Se CTe "123" tem NFEs {"Nota_A"}
/// // E CTe "456" é complementar de "123"
/// // Após a função, CTe "456" terá {"Nota_A"} em suas notas.
/// ```
pub fn expand_cte_nfes(cte_nfes: &mut KeyMap, cte_complementar: &KeyMap) {
    // 1. Acumulador temporário para evitar conflitos de empréstimo (borrow checker)
    // e reduzir a duplicidade de chaves durante o processamento.
    let mut updates: HashMap<String, HashSet<String>> = HashMap::new();

    // 2. Itera sobre os CTes que possuem NFEs
    for (cte, nfes) in cte_nfes.iter() {
        // Se este CTe possui complementares associados...
        if let Some(complements) = cte_complementar.get(cte) {
            for comp in complements {
                // Adiciona todas as NFEs do CTe pai ao CTe complementar no acumulador
                updates
                    .entry(comp.clone())
                    .or_default()
                    .extend(nfes.iter().cloned());
            }
        }
    }

    // 3. Mescla os novos dados acumulados de volta no mapa original.
    // O uso de 'extend' em um HashSet é otimizado internamente.
    for (target_cte, new_nfes) in updates {
        cte_nfes.entry(target_cte).or_default().extend(new_nfes);
    }
}

pub fn get_efd_info(config: &Config) -> SpedResult<HashSet<String>> {
    // 1. Definir delimitador '|'
    let delimiter = b'|';

    // 2. Configuração do Reader (Encapsulada para clareza)
    let file = File::open(&config.efd_path).map_err(|e| SpedError::IoReader {
        source: e,
        arquivo: config.efd_path.clone(),
    })?;

    // 3. Abertura eficiente do arquivo com BufReader aumentado para 128KB
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(true) // O crate gerencia o cabeçalho automaticamente
        .flexible(false) // Garante integridade (erro se o num de colunas variar)
        .trim(csv::Trim::All) // Trim automático em todos os campos
        .buffer_capacity(128 * 1024)
        .from_reader(BufReader::new(file));

    // 4. Obtenção dos nomes das colunas
    let column_names: Vec<&str> = rdr.headers()?.iter().collect();

    // 5. Validação centralizada (verifica se as colunas do config existem no arquivo)
    verificar_existencia_de_colunas_essenciais(
        &column_names,
        TipoDeArquivo::EFDContrib,
        config,
        config.efd_path.clone(),
    )?;

    // 6. Localização da coluna alvo (Chave de 44 dígitos)
    let target_col_name = config
        .colunas_efd
        .get("chave_documento")
        .ok_or_else(|| SpedError::Config("Configuração 'chave_documento' ausente".into()))?;

    // 7. Encontrar a posição da coluna no cabeçalho
    let idx_chave = column_names
        .iter()
        .position(|col| col == target_col_name)
        .ok_or_else(|| SpedError::MissingEssentialColumn {
            arquivo: config.efd_path.clone(),
            coluna: target_col_name.to_string(),
            tipo: TipoDeArquivo::EFDContrib,
        })?;

    // 8. Processamento dos Registros
    let mut keys_efd = HashSet::new();

    // 9. Iteração funcional sobre os registros
    for (idx, result) in rdr.records().enumerate() {
        // Se houver um erro de leitura (incluindo número de colunas errado)
        let record =
            result.map_err(|e| SpedError::from_csv(e, config.efd_path.clone(), idx + 2))?;

        if let Some(content) = record.get(idx_chave) {
            // Limpeza de não-dígitos
            let clean_key = RE_NON_DIGITS.replace_all(content, "");

            if RE_CHAVE_44.is_match(&clean_key) {
                // Transformamos em String apenas uma vez
                let chave = clean_key.into_owned();

                // Primeiro adicionamos chaves correlacionadas (usa a referência &chave)
                add_correlated_keys_to_info(config, &chave, &mut keys_efd);

                // Depois movemos a chave principal para o set (evita .clone())
                keys_efd.insert(chave);
            }
        }
    }

    Ok(keys_efd)
}

fn add_correlated_keys_to_info(config: &Config, chave: &str, info: &mut HashSet<String>) {
    let fontes = [&config.nfe_ctes, &config.cte_nfes, &config.cte_complementar];

    for mapa in fontes {
        if let Some(itens) = mapa.get(chave) {
            info.extend(itens.iter().cloned());
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TipoDeArquivo {
    EFDContrib,
    DocFiscais,
}

pub fn verificar_existencia_de_colunas_essenciais(
    column_names: &[&str],
    tipo: TipoDeArquivo,
    config: &Config,
    input_file: PathBuf,
) -> SpedResult<()> {
    // 1. Validar nomes em branco (Estilo Funcional)
    if column_names.iter().any(|name| name.trim().is_empty()) {
        return Err(SpedError::EmptyColumnName {
            arquivo: input_file,
        });
    }

    // 2. Validar nomes repetidos
    let mut vista = HashSet::with_capacity(column_names.len());

    for name in column_names {
        if !vista.insert(name) {
            return Err(SpedError::DuplicateColumnName {
                arquivo: input_file,
                coluna: name.to_string(),
            });
        }
    }

    // 3. Determinar quais colunas são essenciais para este tipo de arquivo
    let colunas_essenciais = match tipo {
        TipoDeArquivo::EFDContrib => config.colunas_efd.values(),
        TipoDeArquivo::DocFiscais => config.colunas_doc.values(),
    };

    // 4. Validar se todas as colunas essenciais estão presentes (Estilo Funcional)
    // find() retorna a primeira coluna que NÃO está contida no cabeçalho
    if let Some(ausente) = colunas_essenciais
        .into_iter()
        .find(|&essencial| !column_names.contains(essencial))
    {
        return Err(SpedError::MissingEssentialColumn {
            arquivo: input_file,
            coluna: ausente.to_string(),
            tipo,
        });
    }

    // 4. Print informativo (Verbose)
    if config.verbose {
        println!("\nArquivo validado: <{}>", input_file.display());
        println!("Tipo: {:?}", tipo);
        for (i, name) in column_names.iter().enumerate() {
            println!("  coluna [{:02}]: '{}'", i + 1, name);
        }
        println!();
    }

    Ok(())
}

/// Processamento Paralelo de CSVs de Documentos Fiscais
pub fn read_csv_files(config: &Config, keys_efd: &HashSet<String>) -> SpedResult<HashSet<String>> {
    // Usamos AtomicUsize para permitir que múltiplas threads somem o contador sem travar (lock-free)
    let total_itens = AtomicUsize::new(0);

    // O Rayon irá processar os arquivos em paralelo.
    // O flat_map transforma o Stream de (Set, Count) em um Stream único de Chaves.
    let keys_encontradas: HashSet<String> = config
        .arquivos_csv
        .par_iter()
        .flat_map(|path| {
            let (set, count) = process_single_csv(path.to_path_buf(), config, keys_efd)
                .map_err(|e| {
                    eprintln!(" [ERRO] Arquivo <{:?}>: {}", path, e);
                    e
                })
                .unwrap_or_default(); // Se falhar, retorna set vazio e contagem 0

            // Incrementa o contador global de forma segura entre threads
            total_itens.fetch_add(count, Ordering::Relaxed);

            // Transforma o HashSet local em um iterador paralelo para o flat_map
            set.into_par_iter()
        })
        .collect(); // Coleta todas as chaves achatadas no HashSet final

    println!(
        " Total de itens analisados nos documentos fiscais: {}",
        fmt_milhares(total_itens.load(Ordering::Relaxed))
    );

    Ok(keys_encontradas)
}

fn process_single_csv(
    path: PathBuf,
    config: &Config,
    filter: &HashSet<String>,
) -> SpedResult<(HashSet<String>, usize)> {
    let delimiter = b';';

    // 1. Abertura eficiente do arquivo com BufReader aumentado para 128KB
    let file = File::open(path.clone())?;
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(true)
        .flexible(false)
        .trim(csv::Trim::All)
        .buffer_capacity(128 * 1024)
        .from_reader(BufReader::new(file));

    // 4. Obtenção dos nomes das colunas
    let column_names: Vec<&str> = rdr.headers()?.iter().collect();

    // 5. Validação centralizada (verifica se as colunas do config existem no arquivo)
    verificar_existencia_de_colunas_essenciais(
        &column_names,
        TipoDeArquivo::DocFiscais,
        config,
        path.clone(),
    )?;

    // 2. Localização da coluna alvo (Chave de 44 dígitos)
    let target_col_name = config
        .colunas_doc
        .get("chave44_digitos")
        .ok_or_else(|| SpedError::Config("Configuração 'chave44_digitos' ausente".into()))?;

    let target_col_idx = column_names
        .iter()
        .position(|col| col == target_col_name)
        .ok_or_else(|| SpedError::MissingEssentialColumn {
            arquivo: path.clone(),
            coluna: target_col_name.to_string(),
            tipo: TipoDeArquivo::DocFiscais,
        })?;

    // 3. Preparação do Writer temporário com buffer de 1MB para escrita
    let temp_file = {
        let temp_path = config.to_hash(&path);
        File::create(&temp_path)?
    };

    let mut wtr = csv::WriterBuilder::new()
        .delimiter(delimiter)
        .from_writer(BufWriter::with_capacity(1024 * 1024, temp_file));

    // Grava cabeçalho apenas se for o primeiro arquivo
    if config.arquivos_csv.first() == Some(&path) {
        wtr.write_record(rdr.headers()?)?;
    }

    // Fora do loop, alocamos os buffers uma única vez
    let mut record = csv::StringRecord::new(); // Buffer de entrada
    let mut out_record = csv::ByteRecord::new(); // Buffer de saída (reutiliza memória interna)

    let mut found_in_file = HashSet::new();
    let mut count = 0;

    // rdr.read_record preenche o buffer 'record' limpando o conteúdo anterior (sem desalocar)
    while rdr.read_record(&mut record)? {
        count += 1;

        if let Some(content) = record.get(target_col_idx) {
            // OTIMIZAÇÃO 1: Limpeza de dígitos manual (muito mais rápida que Regex em loop)
            let clean_key: String = content.chars().filter(|c| c.is_ascii_digit()).collect();

            // OTIMIZAÇÃO 2: Verificação de tamanho e existência no HashSet
            // clean_key é String, então o .contains() é extremamente eficiente
            if clean_key.len() == 44 && filter.contains(&clean_key) {
                // Inserimos no set de encontrados
                found_in_file.insert(clean_key);

                // OTIMIZAÇÃO 3: Construção da linha de saída sem alocar Vec<String>
                out_record.clear(); // Reseta os índices, mas mantém o buffer de bytes alocado

                for field in record.iter() {
                    // OTIMIZAÇÃO 4: Só chama o Regex se realmente houver espaços duplos
                    if field.contains("  ") {
                        let normalized = RE_MULTISPACE.replace_all(field, " ");
                        out_record.push_field(normalized.as_bytes());
                    } else {
                        // Fast-path: copia os bytes originais diretamente para o buffer de saída
                        out_record.push_field(field.as_bytes());
                    }
                }

                // Escreve o registro completo (o Writer gerencia delimitadores e quebras de linha)
                wtr.write_byte_record(&out_record)?;
            }
        }
    }

    wtr.flush()?;
    Ok((found_in_file, count))
}

pub fn merge_files(config: &Config) -> SpedResult<()> {
    println!(
        "\n Mesclar arquivos temporários em <{}>...\n",
        config.target.display()
    );

    let mut final_file = File::create(&config.target)?;
    let mut seen_lines = HashSet::new();
    let max = config
        .arquivos_csv
        .iter()
        .map(|p| p.display().to_string().len())
        .max()
        .unwrap_or_default();

    for path in &config.arquivos_csv {
        let temp_path = config.to_hash(path);
        println!("{:<max$} -> {temp_path:?}", path.display());

        // Criamos um escopo temporário com { }
        // Tudo o que for aberto aqui dentro será fechado ao chegar no }
        {
            let file = File::open(&temp_path).map_err(|e| SpedError::IoReader {
                source: e,
                arquivo: temp_path.clone().into(),
            })?;

            BufReader::new(file)
                .lines()
                .try_for_each(|line_result| -> SpedResult<()> {
                    let line = line_result?;

                    // Substituir múltiplos espaços por apenas um
                    // replace_all retorna um Cow (Copy-on-Write), que é muito eficiente:
                    // se não houver espaços duplicados, ele apenas referencia a string original.
                    let normalized_line = RE_MULTISPACE.replace_all(&line, " ");

                    let line_hash = blake3::hash(normalized_line.as_bytes()).to_string();

                    if seen_lines.insert(line_hash) {
                        writeln!(final_file, "{}", normalized_line)?;
                    }

                    Ok(())
                })?;

            // O reader e o temp_file morrem aqui automaticamente.
            // Os "file handles" são liberados pelo Sistema Operacional.
        }

        // Remoção segura do arquivo temporário
        if std::path::Path::new(&temp_path).exists() {
            fs::remove_file(&temp_path).map_err(|e| {
                SpedError::Io(e) // O erro de I/O é convertido para nosso SpedError
            })?;
        }
    }

    println!();

    final_file.flush()?;
    Ok(())
}

pub fn exibir_orientacoes_auditoria(config: &Config) {
    println!(
        "\nPesquisar informações do arquivo: {:?}\n",
        config.efd_path
    );
    println!(
        " 1.1 Foram analisadas as chaves NFe/CTe de 44 dígitos contidas na EFD Contribuições."
    );

    // As colunas vêm do nosso LazyLock de colunas estáticas
    let col1 = config.colunas_doc.get("chave44_digitos").unwrap_or(&"N/D");
    let col2 = config.colunas_doc.get("chave_de_acesso").unwrap_or(&"N/D");

    println!("\n Nos Documentos Fiscais de NFe/CTe, há duas colunas principais:");
    println!("  Coluna 1: '{}'", col1);
    println!("  Coluna 2: '{}'\n", col2);

    println!(" 1.2 Foram pesquisadas informações complementares (Transitividade):");
    println!("  - Chaves complementares de CTes (transporte subcontratado).");
    println!("  - NFes vinculadas a CTes com múltiplos documentos (DIVERSOS).");
    println!("  - Estas chaves são obtidas via análise de XML ou chaves complementares.\n");

    println!(" Serão adicionadas ao filtro as chaves onde:");
    println!("  a) NFe está na Coluna 1 dos Docs Fiscais.");
    println!("  b) NFe está na Coluna 2 (casos de CTe com uma única NFe).");
    println!("  c) NFe vinculada a CTe (casos de múltiplos itens obtidos via XML).");
    println!("  d) CTe original e CTe complementar (subcontratação).\n");

    println!(" 2. Analisando chaves nos arquivos de Documentos Fiscais...\n");
}

pub fn imprimir_informacao_segregada(keys: &HashSet<String>, nome: &str, exibir_chaves: bool) {
    // 1. Agrupamento funcional: Código -> Quantidade
    // Usamos BTreeMap para que o loop de impressão seja ordenado pelo código do modelo
    let hash_seg = keys.iter().filter(|key| key.len() >= 22).fold(
        BTreeMap::<String, usize>::new(),
        |mut acc, key| {
            let codigo_doc_fiscal = &key[20..22];
            *acc.entry(codigo_doc_fiscal.to_string()).or_insert(0) += 1;
            acc
        },
    );

    let mut running_sum = 0;

    let max_len = hash_seg
        .keys()
        .map(|codigo| get_modelo_documentos_fiscais(codigo).chars().count())
        .max()
        .unwrap_or_default();

    println!(" --- Relatório de Chaves: {} ---", nome);

    for (codigo, qtd) in &hash_seg {
        let doc_nome = get_modelo_documentos_fiscais(codigo);
        running_sum += qtd;

        println!(
            " Número de chaves em {} (modelo {} : {:<max_len$}) = {:>9} ( soma acumulada = {:>9} )",
            nome,
            codigo,
            doc_nome,
            fmt_milhares(*qtd),
            fmt_milhares(running_sum)
        );
    }

    // Opcional: imprimir as chaves (equivalente ao exibir_chaves do Perl)
    if exibir_chaves {
        println!("\n Detalhamento das chaves:");
        for key in keys {
            println!("  -> {}", key);
        }
    }
    println!();
}

pub fn fmt_milhares(n: usize) -> String {
    let s = n.to_string();
    let len = s.len();
    let mut result = String::with_capacity(len + len / 3);

    s.chars().enumerate().for_each(|(i, c)| {
        // Adiciona o ponto se:
        // 1. Não for o primeiro caractere (i > 0)
        // 2. A distância até o fim for múltipla de 3
        if i > 0 && (len - i).is_multiple_of(3) {
            result.push('.');
        }
        result.push(c);
    });

    result
}

pub fn imprimir_chaves_nao_encontradas(
    keys_efd: &HashSet<String>,
    keys_doc: &HashSet<String>,
) -> HashSet<String> {
    let mut chaves_nao_encontradas = HashSet::new();

    // 1. Segregar todas as chaves por modelo (substr 20, 2)
    let hash_seg = keys_efd.iter().filter(|chave| chave.len() >= 22).fold(
        BTreeMap::<String, HashSet<String>>::new(),
        |mut acc, chave| {
            let codigo_doc_fiscal = &chave[20..22];
            acc.entry(codigo_doc_fiscal.to_string())
                .or_default()
                .insert(chave.clone());
            acc
        },
    );

    let max_len = hash_seg
        .keys()
        .map(|codigo| get_modelo_documentos_fiscais(codigo).chars().count())
        .max()
        .unwrap_or_default();

    println!(" Chaves indicadas em EFD Contribuições e procuradas em Documentos Fiscais:");

    // 2. Iterar pelos modelos ordenados (BTreeMap já provê ordem)
    for (codigo, chaves) in &hash_seg {
        let doc_nome = get_modelo_documentos_fiscais(codigo);
        let num = chaves.len(); // Total de chaves deste modelo

        // Diferença de conjuntos: o que tem na EFD mas não em Documentos Fiscais.
        // Chaves não encontradas segregadas por código do Documento Fiscal.
        let faltantes: HashSet<String> = chaves
            .iter()
            .filter(|&chave| !keys_doc.contains(chave))
            .cloned()
            .collect();

        let sum = faltantes.len(); // chaves não encontradas deste modelo

        chaves_nao_encontradas.extend(faltantes);

        // 3. Print mensagens
        match sum {
            0 => {
                println!(
                    " Número de chaves em EFD Contribuições (modelo {} : {:<max_len$}) = {:>9} sendo que todas foram encontradas nos Documentos Fiscais.",
                    codigo,
                    doc_nome,
                    fmt_milhares(num),
                );
            }
            1 => {
                println!(
                    " Número de chaves em EFD Contribuições (modelo {} : {:<max_len$}) = {:>9} das quais apenas {:>7} não foi encontrada nos Documentos Fiscais.",
                    codigo,
                    doc_nome,
                    fmt_milhares(num),
                    fmt_milhares(sum)
                );
            }
            _ => {
                println!(
                    " Número de chaves em EFD Contribuições (modelo {} : {:<max_len$}) = {:>9} das quais {:>7} não foram encontradas nos Documentos Fiscais.",
                    codigo,
                    doc_nome,
                    fmt_milhares(num),
                    fmt_milhares(sum)
                );
            }
        };
    }

    println!(
        "\n Número total de chaves NÃO encontradas em Documentos Fiscais: {}\n",
        fmt_milhares(chaves_nao_encontradas.len())
    );

    chaves_nao_encontradas
}

/// Exporta chaves de acesso não encontradas para arquivos de texto, segmentando-as
/// por modelo de documento fiscal e limitando a quantidade de linhas por arquivo.
///
/// ### Argumentos
/// * `chaves` - Um `HashSet` contendo as chaves de acesso (strings de 44 caracteres).
/// * `target_base` - O caminho base (prefixo) onde os arquivos serão gerados.
///
/// ### Erros
/// Retorna `SpedResult` em caso de falha na criação ou escrita dos arquivos em disco.
///
/// ### Exemplo de Saída
/// Se o target for `/tmp/falta`, gera arquivos como:
/// `/tmp/falta-NFe-000000.txt`, `/tmp/falta-NFe-000900.txt`, etc.
pub fn exportar_chaves_faltantes(chaves: &HashSet<String>, target_base: &Path) -> SpedResult<()> {
    // Definimos o limite de linhas em um arquivo
    const MAX_LINHAS: usize = 900;

    if chaves.is_empty() {
        return Ok(());
    }

    // --- 1. PREPARAÇÃO E ORDENAÇÃO ---
    // Filtramos chaves válidas e coletamos referências para evitar clonagem desnecessária de strings.
    // Usamos sort_unstable_by_key por ser mais rápido que o sort estável original.
    let mut sorted_chaves: Vec<&String> = chaves.iter().filter(|c| c.len() >= 22).collect();

    sorted_chaves.sort_unstable_by_key(|&c| {
        (
            &c[20..22], // Agrupa por Modelo (NFe 55, CTe 57, etc)
            c,          // Ordena pela chave completa dentro do grupo
        )
    });

    // --- 2. PROCESSAMENTO POR GRUPOS (MODELOS) ---
    // chunk_by separa as chaves toda vez que o modelo (pos 20-22) muda.
    for grupo_modelo in sorted_chaves.chunk_by(|a, b| a[20..22] == b[20..22]) {
        // Extraímos o código do modelo do primeiro elemento do grupo
        let modelo_cod = &grupo_modelo[0][20..22];

        // Obtemos o nome descritivo e limpamos caracteres problemáticos (como o ':')
        let doc_nome = get_modelo_documentos_fiscais(modelo_cod); // .replace(':', "-");

        // --- 3. DIVISÃO EM CHUNKS (ARQUIVOS) ---
        // Para cada modelo, dividimos as chaves em blocos de no máximo 900 linhas.
        for (i, chunk) in grupo_modelo.chunks(MAX_LINHAS).enumerate() {
            // i=0 -> 000000, i=1 -> 000900, i=2 -> 001800, etc.
            let offset = i * MAX_LINHAS;

            let file_path = format!("{}-{}-{:06}.txt", target_base.display(), doc_nome, offset);

            println!(" ---> Novo arquivo de chaves faltantes: <{}>", file_path);

            // Criamos o arquivo e usamos BufWriter para minimizar chamadas de sistema (I/O caro)
            let file = File::create(&file_path)?;
            let mut writer = BufWriter::new(file);

            for chave in chunk {
                // writeln! gerencia a quebra de linha conforme o SO (\n ou \r\n)
                writeln!(writer, "{}", chave)?;
            }

            // O flush garante que os dados saiam do buffer para o disco antes de fechar.
            // O arquivo é fechado automaticamente ao fim deste escopo (RAII).
            writer.flush()?;
        }
    }

    Ok(())
}
