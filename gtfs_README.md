## Descrição dos arquivos GTFS 
---
Os arquivos estão no formato .csv, salvos em .txt

### agency
Informações gerais sobre as agências responsáveis pelo serviço de transporte.

| Nome da Coluna  | Descrição                                                                 | Exemplo                                   |
|-----------------|---------------------------------------------------------------------------|-------------------------------------------|
| agency_id       | Identificador único da agência de transporte.                             | 1                                         |
| agency_name     | Nome da agência responsável pelo serviço.                                 | SPTRANS                                   |
| agency_url      | URL do site oficial da agência.                                           | http://www.sptrans.com.br/?versao=091025 |
| agency_timezone | Fuso horário utilizado pela agência.                                      | America/Sao_Paulo                         |
| agency_lang     | Código do idioma utilizado pela agência (ISO 639-1).                      | pt                                        |

---

### calendar
Define os dias da semana e o período em que um serviço está ativo.

| Nome da Coluna | Descrição                                                                 | Exemplo  |
|----------------|---------------------------------------------------------------------------|-----------|
| service_id     | Identificador do serviço (conjunto de dias em que há operação).           | USD       |
| monday         | Indica operação na segunda-feira (`1` = sim, `0` = não).                  | 1         |
| tuesday        | Indica operação na terça-feira.                                           | 1         |
| wednesday      | Indica operação na quarta-feira.                                          | 1         |
| thursday       | Indica operação na quinta-feira.                                          | 1         |
| friday         | Indica operação na sexta-feira.                                           | 1         |
| saturday       | Indica operação no sábado.                                                | 1         |
| sunday         | Indica operação no domingo.                                               | 1         |
| start_date     | Data de início da vigência do serviço (AAAAMMDD).                         | 20231001  |
| end_date       | Data de término da vigência do serviço (AAAAMMDD).                        | 20260501  |

---

### fare_attributes
Informações detalhadas sobre as tarifas cobradas no transporte.

| Nome da Coluna   | Descrição                                                                 | Exemplo   |
|------------------|---------------------------------------------------------------------------|-----------|
| fare_id          | Identificador único da tarifa.                                            | Ônibus    |
| price            | Valor da tarifa.                                                          | 5.000000  |
| currency_type    | Moeda utilizada (ISO 4217).                                               | BRL       |
| payment_method   | Indica se o pagamento é feito antes (`0`) ou durante (`1`) a viagem.     | 0         |
| transfers        | Número de transferências permitidas.                                      | (vazio)   |
| transfer_duration| Duração máxima de transferência em segundos.                              | 10800     |

---

### fare_rules
Regras que associam tarifas específicas a rotas ou zonas.

| Nome da Coluna  | Descrição                                                     | Exemplo   |
|-----------------|---------------------------------------------------------------|-----------|
| fare_id         | Identificador da tarifa associada à regra.                    | CPTM      |
| route_id        | Identificador da linha ou rota à qual a regra se aplica.      | CPTM L07  |
| origin_id       | Identificador da zona de origem (opcional).                   | (vazio)   |
| destination_id  | Identificador da zona de destino (opcional).                  | (vazio)   |
| contains_id     | Identificador de zona intermediária incluída (opcional).      | (vazio)   |

---

### frequencies
Define intervalos regulares de partida para uma viagem dentro de um período.

| Nome da Coluna | Descrição                                                       | Exemplo     |
|----------------|-----------------------------------------------------------------|-------------|
| trip_id        | Identificador da viagem à qual a frequência se aplica.          | 1012-10-0   |
| start_time     | Horário inicial do intervalo em que a frequência é válida (HH:MM:SS). | 00:00:00    |
| end_time       | Horário final do intervalo em que a frequência é válida (HH:MM:SS).   | 00:59:00    |
| headway_secs   | Intervalo entre partidas em segundos (ex.: 1200 = 20 minutos).  | 1200        |

---

### routes
Informações sobre as rotas/linhas operadas pelas agências.

| Nome da Coluna    | Descrição                                                    | Exemplo                           |
|-------------------|--------------------------------------------------------------|-----------------------------------|
| route_id          | Identificador único da rota/linha.                           | 1012-10                         |
| agency_id         | Identificador da agência que opera a rota.                   | 1                               |
| route_short_name  | Nome curto ou número da rota (ex.: linha 1012-10).           | 1012-10                         |
| route_long_name   | Nome completo ou descrição da rota.                           | Term. Jd. Britania - Jd. Monte Belo |
| route_type        | Tipo da rota (ex.: 3 = ônibus).                             | 3                               |
| route_color       | Cor da rota em hexadecimal (sem #).                          | 509E2F                          |
| route_text_color  | Cor do texto da rota em hexadecimal (sem #).                 | FFFFFF                          |

---

### shapes
Define os pontos geográficos que compõem o trajeto de uma rota.

| Nome da Coluna    | Descrição                                                   | Exemplo       |
|-------------------|-------------------------------------------------------------|---------------|
| shape_id          | Identificador único da forma/trajeto da rota.               | 84609         |
| shape_pt_lat      | Latitude do ponto da forma, em graus decimais (WGS84).      | -23.432024    |
| shape_pt_lon      | Longitude do ponto da forma, em graus decimais (WGS84).     | -46.787121    |
| shape_pt_sequence | Ordem do ponto na forma (sequência).                        | 1             |
| shape_dist_traveled| Distância percorrida até esse ponto em metros (opcional).   | 0             |

---

### stop_times
Horários e sequência dos pontos de parada dentro de uma viagem.

| Nome da Coluna   | Descrição                                                                 | Exemplo      |
|------------------|---------------------------------------------------------------------------|--------------|
| trip_id          | Identificador da viagem à qual o horário pertence.                       | 1012-10-0    |
| arrival_time     | Horário de chegada ao ponto, no formato HH:MM:SS.                        | 07:00:00     |
| departure_time   | Horário de partida do ponto, no formato HH:MM:SS.                        | 07:00:00     |
| stop_id          | Identificador do ponto de parada (referência ao arquivo `stops.txt`).    | 301790       |
| stop_sequence    | Ordem do ponto dentro da viagem (1 = primeiro ponto).                    | 1            |

---

### stops
Informações sobre os pontos ou estações de parada disponíveis.

| Nome da Coluna | Descrição                                                                 | Exemplo      |
|----------------|---------------------------------------------------------------------------|--------------|
| stop_id        | Identificador único do ponto ou estação de parada.                       | 18848        |
| stop_name      | Nome do ponto de parada visível ao público.                              | Clínicas     |
| stop_desc      | Descrição adicional do ponto (opcional).                                 | (vazio)      |

---

### trips
Detalhes das viagens específicas realizadas em uma rota.

| Nome da Coluna  | Descrição                                                                 | Exemplo        |
|-----------------|---------------------------------------------------------------------------|----------------|
| route_id        | Identificador único da linha ou rota de transporte.                      | 1012-10        |
| service_id      | Referência ao tipo de serviço (ex.: dias úteis, fins de semana, feriados).| USD            |
| trip_id         | Identificador único de uma viagem específica na rota.                    | 1012-10-0      |
| trip_headsign   | Texto mostrado aos passageiros indicando o destino ou direção da linha.  | Jd. Monte Belo |
| direction_id    | Direção da viagem: normalmente `0` para ida e `1` para volta.            | 0              |
| shape_id        | Identificador que referencia o trajeto geográfico (forma) da viagem.     | 84609          |
