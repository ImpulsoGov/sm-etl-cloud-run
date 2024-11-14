# sm-etl-cloud-run

<!--
SPDX-FileCopyrightText: 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
-->
<!-- ![Badge DBT](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Badge em Desenvolvimento](https://img.shields.io/badge/status-em%20desenvolvimento-yellow) -->

# Impulso Saúde Mental | ETL de dados públicos

Este repositório contém os ETLs de dados públicos relevantes para a plataforma
de Indicadores de Saúde Mental.

_**Nota**: Esse repositório substitui os processos legado do repo
[@ImpulsoGov/etl][scripts legados].

[scripts legados]: https://github.com/ImpulsoGov/etl/

*******
## :mag_right: Índice
1. [Contexto](#contexto)
2. [Estrutura do repositório](#estrutura)
4. [Instruções para instalação](#instalacao)
    1. [Antes da instalação](#preinstalacao)
    2. [Obtendo e construindo a imagem Docker](#imagemdocker)
5. [Utilização](#utilizacao)
    1. [Local direta](#u_local)
    2. [Local via flask](#u_local_flask)
    3. [Na GCP](#u_gcp)
6. [Contribua](#contribua)
7. [Licença](#licenca)
*******

<div id='contexto'/>  

## :rocket: Contexto

Um dos propósitos da ImpulsoGov, enquanto organização, é transformar dados da saúde pública do Brasil em informações que ofereçam oferecer suporte de decisão aos gestores de saúde pública em todo o Brasil. Embora o SUS tenha uma riqueza de dados há muitas dificuldades para reunir, coletar e analisar dados em diferentes sistemas de informação. O projeto de Saúde Mental tem como objetivo apoiar gestões da Rede de Atenção Psicossocial (RAPS) com uso de dados e indicadores desenvolvidos a partir do entendimento do dia a dia e dos principais desafios da rede. Hoje, este repositório estrutura o ETL de dados públicos, que são utilizados para a geração de indicadores da Plataforma de Indicadores de Saúde Mental.
*******

<div id='estrutura'/>  
 
## :milky_way: Estrutura do repositório

```plain
├── dockerfile
├── README.md
├── sm_cloud_run
│   ├── app.py
│   ├── etl
│   │   ├── datasus_ftp_metadados.py
│   │   ├── scnes_habilitacoes.py
│   │   ├── scnes_vinculos.py
│   │   ├── siasus_bpa_individualizado.py
│   │   ├── siasus_procedimentos_ambulatoriais.py
│   │   ├── siasus_raas_ps.py
│   │   ├── sihsus_aih_rd.py
│   │   ├── sisab_resolutividade_por_condicao.py
│   │   └── sisab_tipo_equipe_por_tipo_producao.py
│   ├── load_bd
│   │   ├── scnes_habilitacoes_load_bd.py
│   │   ├── scnes_vinculos_load_bd.py
│   │   ├── siasus_bpa_individualizado_load_bd.py
│   │   ├── siasus_procedimentos_ambulatoriais_load_bd.py
│   │   ├── siasus_raas_ps_load_bd.py
│   │   ├── sihsus_aih_rd_load_bd.py
│   │   ├── sisab_resolutividade_por_condicao_load_bd.py
│   │   └── sisab_tipo_equipe_por_tipo_producao_load_bd.py
│   ├── utilitarios
│   │   ├── bd_config.py
│   │   ├── bd_utilitarios.py
│   │   ├── cloud_storage.py
│   │   ├── config_painel_sm.py
│   │   ├── datas.py
│   │   ├── datasus_ftp.py
│   │   ├── geografias.py
│   │   ├── logger_config.py
│   │   ├── sisab_producao_modelos.py
│   │   ├── sisab_relatorio_producao_utilitarios.py
│   │   └── tipos.py
│   ├── scripts
│   │   ├── build_n_push.sh
│   │   ├── criar-artifact-repository.sh
│   │   └── verificar_e_executar.py
│   └── requirements.txt
└──
```

- `etl` contém os scripts da Etapa1 do ETL (extração da fonte de dados original, tratamento e carregamento no Google Cloud Storage);
- `load_bd` contém os scripts da Etapa2 do ETL (extração do GCS, tratamento e carregamento em banco de dados Postgres);
- `utilitarios` contém funções utilitárias que são recorrentes nas etapas 1 e 2 do ETL
- `scripts` armazena scripts de criação e upload para nuvem de imagem Docker, criação de artefatos e script auxiliar de execução do CloudRunService
*******

<div id='instalacao'/> 

## 🛠️ Instalação

 <div id='preinstalacao'/> 
 
 ### Antes da instalação
 
 As ferramentas presentes neste repositório pressupõem que a máquina em questão
 possui o [Docker][] instalado para a execução de contêineres. Rodar a aplicação
 fora do contêiner Docker providenciado junto ao repositório pode levar a
 resultados inesperados.
 
 A aplicação espera que o seu sistema contenha informações importantes para 
 a conexão com a instância do banco de dados. Com a estrutura atual de código 
 todas as informações de conexão estão armazenadas externamente em ambiente de nuvem
 (Google Secrets Manager), mas também é possível alterar o script `utilitarios.bd_config.py`
 para que um arquivo de variável de ambiente seja aceito. 
 Veja as variáveis esperadas no arquivo [`.env.sample`][].
 
 Mais informações sobre a criação de uma instância de banco
 de dados com estrutura semelhante à da ImpulsoGov podem ser encontrados no
 repositório [@ImpulsoGov/bd][].
 
 [@ImpulsoGov/bd]: https://github.com/ImpulsoGov/bd
 


<div id='utilizacao'/> 

## :gear: Utilização
 
 <div id='u_local'/> 
 
 ### Execução local direta
 Para executar localmente o script de algum ETL (etapa 1 ou 2) é possível
 invocar diretamente via Python o arquivo desejado, alterando na função final
 do arquivo fonte os parâmetros da UF e competência desejados.

<div id='u_local_flask'/> 
 
 ### Execução com flask
 Para executar localmente a partir de uma requisição utilizando o Flask,
 conecte-se a uma sessão local do Flask dentro da pasta `sm_cloud_run` e 
 forneça os parâmetros para a execução.

 Para executar em ambiente de nuvem, obtenha o endereço web da aplicação e
 substitua na requisição POST.
 
 Exemplo execução da Etapa1 do ETL, para arquivo de Habilitações do Acre na competência de 
 processamento 2024-08-01.

 ```sh
 curl --location --request POST 'http://link/datasus_etl_e_load_habilitacoes' \
    --header 'Content-Type: application/json' \
    --data '{
     "UF": 'AC', 
     "data": '2024-08-01',
     "ETL": "HB",
     "acao": "baixar"
 }'
 ```

 - Parâmetro ETL: Aceita valores 'PA', 'BI', 'PS', 'RD', 'HB' ou 'PF'
 - Parâmetro acao: Aceita valores 'baixar' (referente a Etapa1 do ETL) ou 'inserir' (referente a Etapa2 do ETL)
 - Para consultar quais rotas devem ser utilizadas para cada tipo de ETL, acesse o arquivo `app.py`


*******

<div id='contribua'/>  

## :left_speech_bubble: Contribua
Sinta-se à vontade para contribuir em nosso projeto! Abra uma issue ou envie PRs.

*******
<div id='licenca'/>  

## :registered: Licença
MIT © (?)
