use execution_time::ExecutionTime;
use std::process;

use reter_linhas_com_info_das_chaves::{
    SpedResult, clear_screen, exibir_orientacoes_auditoria, expand_cte_complementar,
    expand_cte_nfes, exportar_chaves_faltantes, get_config, get_efd_info, get_nfe_ctes,
    imprimir_chaves_nao_encontradas, imprimir_informacao_segregada, imprimir_versao_do_programa,
    ler_chave_complementar_deste_cte, ler_todas_as_nfes_deste_cte, merge_files, read_csv_files,
};

fn main() {
    // A forma mais idiomática de reportar erros ao usuário final sem stack trace técnico
    if let Err(err) = run() {
        eprintln!("\n[ERRO CRÍTICO]: {err}");
        process::exit(1);
    }
}

fn run() -> SpedResult<()> {
    let timer = ExecutionTime::start();

    // 1. Obter Configurações
    let mut config = get_config()?;

    // 2. Setup inicial
    clear_screen(config.clear)?;
    imprimir_versao_do_programa();

    println!("Iniciando processamento SPED EFD em Rust...\n");

    // 3. Carregamento de Relacionamentos (Lógica funcional)
    let file_cte = "cte_nfes.txt";
    let mut cte_nfes = ler_todas_as_nfes_deste_cte(file_cte)?;

    let file_comp = "transporte_subcontratado-chaves_complementares_dos_CTes.txt";
    let mut cte_complementar = ler_chave_complementar_deste_cte(file_comp)?;

    // 4. Expansão das relações (Transitividade)
    expand_cte_complementar(&mut cte_complementar);

    // 5. Propagação de NFes para CTes complementares
    expand_cte_nfes(&mut cte_nfes, &cte_complementar);

    // 6. Geração do índice invertido (NFe -> CTes)
    let nfe_ctes = get_nfe_ctes(&cte_nfes);

    // 7. Injetar informações no config para uso em get_efd_info
    config.nfe_ctes = nfe_ctes;
    config.cte_nfes = cte_nfes;
    config.cte_complementar = cte_complementar;

    if config.verbose {
        println!("{:#?}\n", config);
    }

    // 8. Processamento EFD
    let keys_efd = get_efd_info(&config)?;

    // 9. Exibir orientações e estatísticas da EFD
    exibir_orientacoes_auditoria(&config);
    imprimir_informacao_segregada(&keys_efd, "EFD Contribuições", config.efd_keys);

    // 10. Processamento Documentos Fiscais (Paralelo)
    let keys_doc = read_csv_files(&config, &keys_efd)?;

    // 11. Consolidação
    merge_files(&config)?;
    imprimir_informacao_segregada(&keys_doc, "Documentos Fiscais", config.docs_keys);

    // 12. Relatório Final de Ausências
    let chaves_faltantes = imprimir_chaves_nao_encontradas(&keys_efd, &keys_doc);

    if !chaves_faltantes.is_empty() {
        exportar_chaves_faltantes(&chaves_faltantes, &config.target)?;
    }

    println!(" Auditoria concluída com sucesso.\n");
    timer.print_elapsed_time();

    Ok(())
}
