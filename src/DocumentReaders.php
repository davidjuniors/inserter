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
    
    private bool $endOfFile = false;

    /**
     * @param $path - Caminho dos arquivos dos quais o script irá ler.
     */
    public function __construct(string $path, string $table)
    {
        $this->documentsPath = $path;

        $this->tableValues = [
            'cnpj', 
            'tipo_devedor_id', 
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
        $this->conn = new Connection('scan-tributario', 'postgres', 'postgres', 'localhost');

        foreach($files as $file) {
            if (is_file($this->documentsPath . "/" . $file))
                $this->prepareToInsert($file);
        }
    }

    private function prepareToInsert(string $fileName)
    {
        $handle  = fopen($this->documentsPath . "/" . $fileName, "r");

        $start = microtime(true);

        $i = 0;
        if ($handle) {
            while(!feof($handle)) {
                $data = fgetcsv($handle, 0, ";");

                if ($data[0] == "CPF_CNPJ")
                    continue;

                if ($data[1] == "Pessoa física")
                    continue;
                
                $this->databaseInsert($data);
                $i++;
            }
        }

        $this->endOfFile = true;
        fclose($handle);

        $end = microtime(true);
        $timeExecution = $end - $start;

        echo sprintf("Script executed in % seconds.\n", number_format($timeExecution, 2));
    }

    private function databaseInsert(array $data)
    {
        var_dump($data); die;

        $estados = ['RO' => 11,
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

        $cnpj = $data[0]; 
        $tipo_devedor_id = $data[2]; 
        $nome_devedor = $data[3]; 
        $uf_id = $estados[$data[4]]; 
        $numero_inscrição = $data[6]; 

        $tipo_inscricao_id = $data[7]; 
        $situacao_inscricao_id = $data[8]; 
        $receita_principal_id = $data[9]; 
        $data_inscricao = date_create($data[10]); 
        $indicador_ajuizado = $data[11] == 'SIM' ? true : false; 
        $valor = $data[12];

        
        $sql = sprintf("INSERT INTO %s (%s) VALUES ", $this->tableName, implode(", ", $this->tableValues));


        $this->arrayInsert .= sprintf("(%s)", $data);

        if ($i % 15000 == 0) {
            $this->insertDatabase($sql);
        }

        if ($this->endOfFile) {
            $this->insertDatabase($sql);
        }

    }

    private function insertDatabase(string $base): void
    {
        pg_query($this->conn, $base . $this->arrayInsert);
        $this->arrayInsert = array();
    }
}