# ExecuteDynamoDB

 Processador customizado para ajudar nas funcionalidades do DynamoDB da AWS.

1.0.0 (31/03/2022) 
    - Finalizado a primeira versão do processador.

1.0.1 (03/06/2022) 
    - TKSIN-807 - Ajustando erro de null quando não acha item na função GetItem

1.0.2 (14/07/2022) 
    - TKSIN-833 - Ajustado erro de requisição de context ao chamar a função Getitem

1.0.3 (29/08/2022) 
    - TKSIN-906 - Entender os builds errados dos NAR's

1.0.3 (30/08/2022) 
    - Adicionado método de Scan para o processador, relação de split para as respostas do scan

1.0.3 (20/09/2022)
    - Removido a relação de SPLIT para as respostas de Scan e Query
    - Adicionado campo de Last Evaluated Key para o método Scan

Release 1.0.4 (27/09/2022)
    - Atualizado método Update Item para receber um JSON com todos os campos que devem ser alterados e atualiza-los em uma única requisição.