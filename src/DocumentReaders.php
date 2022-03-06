<?php

namespace DavidJuniors\Inserter;
use DavidJuniors\Inserter\Connection;

class DocumentReaders
{
    public $documentsPath;
    public $conn;
    public $tableName;
    public $tableValues;
    public $arrayInsert = array();

    public $ins_search;
    public $sit_search;
    public $rec_search;
    
    private bool $endOfFile = false;

    /**
     * @param $path - Caminho dos arquivos dos quais o script irá ler.
     */
    public function __construct(string $path, string $table)
    {
        $this->documentsPath = $path;

        $this->tableValues = [
            'cnpj', 
            'cnpj_base', 
            'devedor_principal', 
            'nome_devedor', 
            'uf_id', 
            'numero_inscrição', 
            'tipo_inscricao_id', 
            'situacao_inscricao_id', 
            'receita_principal_id', 
            'data_inscricao', 
            'indicador_ajuizado', 
            'valor', 
        ];

        $this->tableName = $table;
        $this->readDocuments();
    }
    
    public function readDocuments(): void
    {
        $files = scandir($this->documentsPath);
        //$this->conn = new Connection('tributario', 'postgres', 'postgres', 'localhost');
        //$this->conn = pg_connect('host=localhost  port=5432 dbname=tributario user=postgres password=postgres');
        $this->conn = pg_connect("host=localhost  port=5432 dbname=rfb_database user=forge password=cKSEbnqj97NvrJJScysb");

        $inscricao = pg_query($this->conn, 'SELECT id, nome FROM divida_tipo_inscricoes');
        $inscricao_arr = pg_fetch_all($inscricao);
        foreach($inscricao_arr as $key) {
            $this->ins_search[$key['nome']] = $key['id'];
        }
        
        $situacao = pg_query($this->conn, 'SELECT * FROM divida_tipo_situacoes');
        $situacao_arr = pg_fetch_all($situacao);
        foreach($situacao_arr as $key) {
            $this->sit_search[$key['nome']] = $key['id'];
        }

        $receita = pg_query($this->conn, 'SELECT * FROM divida_receita_principal');
        $receita_arr = pg_fetch_all($receita);
        foreach($receita_arr as $key) {
            $this->rec_search[$key['nome']] = $key['id'];
        }

        foreach($files as $file) {
            if (is_file($this->documentsPath . "/" . $file))
                $this->prepareToInsert($file);
        }
    }

    private function prepareToInsert(string $fileName)
    {
        echo "Preparing $fileName to insert in DataBase" . PHP_EOL;

        $fhandle  = fopen($this->documentsPath . "/" . $fileName, "r");

        $start = microtime(true);

        $i = 0;
        if ($fhandle) {
            while(!feof($fhandle)) {
                $data = fgetcsv($fhandle, 0, ";");

                if (!is_array($data))
                    continue;

                if ($data[0] == "CPF_CNPJ")
                    continue;

                if (utf8_encode($data[1]) == "Pessoa física")
                    continue;
                
                $this->databaseInsert($data);
                $i++;
            }
        }

        $this->endOfFile = true;
        fclose($fhandle);

        $end = microtime(true);
        $timeExecution = $end - $start;

        echo sprintf("Script executed in % seconds.\n", number_format($timeExecution, 2));
    }

    private function databaseInsert(array $data)
    {
        //var_dump($data); //die;

        $estados = [
            'RO' => 11,
            'AC' => 12,
            'AM' => 13,
            'RR' => 14,
            'PA' => 15,
            'AP' => 16,
            'TO' => 17,
            'MA' => 21,
            'PI' => 22,
            'CE' => 23,
            'RN' => 24,
            'PB' => 25,
            'PE' => 26,
            'AL' => 27,
            'SE' => 28,
            'BA' => 29,
            'MG' => 31,
            'ES' => 32,
            'RJ' => 33,
            'SP' => 35,
            'PR' => 41,
            'RS' => 43,
            'MS' => 50,
            'MT' => 51,
            'GO' => 52,
            'DF' => 53,
            'SC' => 42
    ];

        $i = 0;

        $cnpj = str_replace([".", "-", "/", " "], "", $data[0]); 
        $devedor_principal = $data[2] == 'PRINCIPAL' ? 'true' : 'false'; 
        $nome_devedor = utf8_encode($data[3]); 
        $uf_id = $estados[$data[4]]; 
        $numero_inscrição = $data[6]; 

        $fix_inscricao = key_exists(utf8_encode($data[7]), $this->ins_search) ? $this->ins_search[utf8_encode($data[7])] : 99999;
        $fix_situacao = key_exists(utf8_encode($data[8]), $this->sit_search) ? $this->sit_search[utf8_encode($data[8])] : 99999;
        $fix_receita = key_exists(utf8_encode($data[9]), $this->rec_search) ? $this->rec_search[utf8_encode($data[9])] : 99999;

        $tipo_inscricao_id = $fix_inscricao; 
        $situacao_inscricao_id = $fix_situacao; 
        $receita_principal_id = $fix_receita; 

        $data_inscricao = str_replace("/", "-", $data[10]); 
        $indicador_ajuizado = stristr("SIM", $data[11]) ? 'true' : 'false'; 
        $valor = str_replace(".", "", $data[12]);

        $this->arrayInsert[] = sprintf("(%s, %d, %s, '%s', %s, %s, %s, %s, %s, '%s', %s, %s)", 
                                $cnpj, 
                                substr($cnpj, 0, 8), 
                                $devedor_principal, 
                                pg_escape_string($this->conn, $nome_devedor), 
                                $uf_id, 
                                $numero_inscrição, 
                                $tipo_inscricao_id, 
                                $situacao_inscricao_id, 
                                $receita_principal_id, 
                                $data_inscricao, 
                                $indicador_ajuizado, 
                                $valor
                            );
        //var_dump($this->arrayInsert); die;

        if ($i % 15000 == 0) {
            $this->insertDatabase();
        }

        if ($this->endOfFile) {
            $this->insertDatabase();
        }

    }

    private function insertDatabase(): void
    {
        pg_query($this->conn, 'start transaction;');
        $base = sprintf("INSERT INTO %s (%s) VALUES ", $this->tableName, implode(", ", $this->tableValues));
        if (!empty($this->arrayInsert)) {
            $sql = $base . implode(', ',$this->arrayInsert);
            pg_query($this->conn,$sql);
            $this->arrayInsert = array();
        }
        pg_query($this->conn,'commit;');
    }
}