<?php

namespace DavidJuniors\Inserter;

use PDO;
use PDOException;

class Connection
{
    private $database = 'scan-tributario';
    private $user = 'postgres';
    private $password = 'postgres'; // change to your password
    private $host= 'localhost';

    public function __construct(string $database, string $user, string $password, string $host = 'localhost')
    {
        $this->database = $database;
        $this->user = $user;
        $this->password = $password;
        $this->host = $host;

        $this->createConnection();
    }
    
    private function createConnection()
    {
        try {
            $dsn = sprintf("pgsql:host=%s;port=5432;dbname=%s;", $this->host, $this->database);
            // make database connection
            $pdo = new PDO($dsn, $this->user, $this->password, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        
            if ($pdo) {
                echo "Connected to the {$this->database} database successfully!\n";
                return $pdo;
            }
        } catch (PDOException $e) {
            die($e->getMessage());
        } finally {
            if ($pdo) {
                $pdo = null;
            }
        }
    }
}