/*
O algoritmo do dígito verificador (DV) da chave de acesso da NF-e (Nota Fiscal Eletrônica)
usa o método módulo 11, multiplicando os 43 primeiros dígitos da chave (da direita para a esquerda)
por pesos de 2 a 9 (2, 3, 4, 5, 6, 7, 8, 9, 2, 3...) para somar os resultados e encontrar
o resto da divisão por 11; o DV é 11 menos esse resto, sendo 0 se o resto for 0 ou 1,
garantindo a integridade da chave.
*/

/// Erro no Dígito Verificador
#[derive(Debug, PartialEq, Eq)]
pub enum DVError {
    FormatoInvalido,
    CaractereInvalido,
    DigitoVerificadorIncorreto,
    TamanhoInvalido,
}

/// Calcula o Dígito Verificador (DV) para o corpo de uma chave (43 dígitos).
///
/// Implementação funcional de alta performance:
/// - Zero alocação de memória (não cria Vec ou Strings temporárias).
/// - Short-circuiting: para no primeiro erro encontrado.
pub fn calcular_dv(corpo: &str) -> Result<u32, DVError> {
    // Validação rigorosa de tamanho para o cálculo
    if corpo.len() != 43 {
        return Err(DVError::TamanhoInvalido);
    }

    // Algoritmo Módulo 11 (Pesos de 2 a 9 da direita para a esquerda)
    // try_fold é a forma idiomática de reduzir um iterador que pode falhar
    let soma = corpo
        .chars()
        .rev() // Começa da direita (posição 43) para a esquerda
        .enumerate()
        .try_fold(0u32, |acc, (i, c)| {
            let digito = c.to_digit(10).ok_or(DVError::CaractereInvalido)?;
            let peso = (i as u32 % 8) + 2; // Ciclo 2, 3, 4, 5, 6, 7, 8, 9...
            Ok(acc + (digito * peso))
        })?;

    let resto = soma % 11;

    // Regra da Receita Federal: Se resto é 0 ou 1, DV é 0. Senão, 11 - resto.
    Ok(if resto < 2 { 0 } else { 11 - resto })
}

/// Valida a integridade de uma Chave de Acesso de NFe ou CTe (44 dígitos).
pub fn validar_chave_acesso(chave: &str) -> Result<(), DVError> {
    // 1. Garante exatamente 44 bytes e que todos os caracteres são dígitos numéricos ASCII ('0'..='9')
    if chave.len() != 44 || !chave.bytes().all(|b| b.is_ascii_digit()) {
        return Err(DVError::FormatoInvalido);
    }

    // 2. Fatiamento 100% seguro (O(1)) garantido sem panics.
    // Se a chave for menor que 43 caracteres, retorna um erro controlado
    let (corpo, dv_informado_str) = chave.split_at_checked(43).ok_or(DVError::TamanhoInvalido)?;

    // Converte o último dígito da string para u32 para comparação
    let dv_informado = dv_informado_str
        .chars()
        .next()
        .and_then(|c| c.to_digit(10))
        .ok_or(DVError::CaractereInvalido)?;

    // Calcula o DV esperado com base nos primeiros 43 dígitos
    let dv_calculado = calcular_dv(corpo)?;

    if dv_calculado == dv_informado {
        Ok(())
    } else {
        Err(DVError::DigitoVerificadorIncorreto)
    }
}

//----------------------------------------------------------------------------//
//                                   Tests                                    //
//----------------------------------------------------------------------------//
//
// cargo test -- --help
// cargo test -- --nocapture
// cargo test -- --show-output

/// Run tests with:
/// cargo test -- --show-output tests_digito_verificador
#[cfg(test)]
mod tests_digito_verificador {
    use super::*;

    // Chave de exemplo (NFe real fictícia para teste)
    const CHAVE_NFE_VALIDA: &str = "35170112345678000190550010000000011000000017";

    #[test]
    fn test_verificar_chave() -> Result<(), DVError> {
        // Exemplo de chave (NFe ou CTe fictícia)
        let chave = CHAVE_NFE_VALIDA;
        println!("chave: {chave}");

        // Calcula o DV esperado com base nos primeiros 43 dígitos
        let corpo = &CHAVE_NFE_VALIDA[..43];
        let dv_calculado = calcular_dv(corpo)?;

        println!("dv_calculado: {dv_calculado}");

        match validar_chave_acesso(chave) {
            Ok(_) => println!("✅ Chave válida!"),
            Err(e) => println!("❌ Chave inválida: {:?}", e),
        }

        assert_eq!(dv_calculado, 7);

        Ok(())
    }

    #[test]
    fn test_calculo_dv_sucesso() {
        let corpo = &CHAVE_NFE_VALIDA[..43];
        assert_eq!(calcular_dv(corpo), Ok(7));
    }

    #[test]
    fn test_chave_valida_completa() {
        assert!(validar_chave_acesso(CHAVE_NFE_VALIDA).is_ok());
    }

    #[test]
    fn test_chave_tamanho_errado() {
        assert_eq!(validar_chave_acesso("123"), Err(DVError::FormatoInvalido));
    }

    #[test]
    fn test_chave_com_letras() {
        // Substituindo um dígito por 'A'
        let chave_invalida = "3517011234567800019055001000000001100000001A";
        assert_eq!(
            validar_chave_acesso(chave_invalida),
            Err(DVError::FormatoInvalido)
        );
    }

    #[test]
    fn test_dv_incorreto() {
        // Chave com o último dígito alterado propositalmente de 7 para 5
        let chave_corrompida = "35170112345678000190550010000000011000000015";
        assert_eq!(
            validar_chave_acesso(chave_corrompida),
            Err(DVError::DigitoVerificadorIncorreto)
        );
    }
}
