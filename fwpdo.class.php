<?php
/*
Date        Ver   Who  Change
----------  ----- ---  -----------------------------------------------------
            1.0   FHO  Class isolated from existing project
2015-08-14  1.1   FHO  camelCase
2016-05-30  1.2   FHO  added some prototyping
                       added recording functions
                       added transactionCount to avoid subtransactions (mySql does not support subtransactions
                       BEGIN TRANS inside a transaction will commit current one and start a new one)
            1.3   FHO  mssql, pgsql
            1.4   FHO  amelioration de la compatibilite ascendante
                       __construct: support des anciens formats a 8 parametres
                       fwpdo::setwarnchannel()
2018-10-07  1.5   FHO  getHost(), get_host(), getAttribute()
2018-10-10  1.6   FHO  - select1()
                       - remonte une erreur si 'from' est vide
2018-10-18  1.7   FHO  - update accepte 'join'
2018-11-06  1.7.1 FHO  - improved an error message
2018-11-29  1.8   FHO  - setCharset(): utf-8 changed to utf8 (mysql)
                       - getCharset(), getCollation()
                       - refactoring of buildLimitStatement(), can now paginate MSSQL and PGSQL
                       - added missing protos
2018-12-14  1.8.1 FHO  bugfix: 'group' -> 'groupby'
2018-12-14  1.9   FHO  - select() now accepts a string with SQL request: select('SELECT...')
                       - new: describe()
2018-12-28  1.10  FHO  bugfix: delete() returned true even when failed
                       new: delete1()
2019-01-07  1.11  FHO  fwpdo::fetch() uses fetchAll(), no longer iterates on fetch()
                       added comments

Known issues
--------------
1- si une table ne possede pas de PK AUTOINC, alors on ne peut pas recuperer d'insert id apres l'insertion
   actuellement, cela leve une exception meme si l'insertion s'est deroule avec succes

added basic support for pgsql, mssql
added limit for update

*/

// Database connexion pool
// example:
// $dbo = dbCnxPool::getPDO([ 'engine' => 'mysql', 'host' => 'localhost', 'username' => 'dbuser', 'passwd' => 'dbpassword', 'database' => 'mdb']);
// $dbo = dbCnxPool::getPDO([ 'dsn' => 'mysql:dbname=mdb;host=localhost', 'username' => 'dbuser', 'passwd' => 'dbpassword' ]);
// at first call, will create a PDO object
// subsequent calls for same 'database' value will return previously created object

require_once 'errorhandler.class.php';

class dbCnxPool
{
	private static $dbLinks;
	private static $lastError;	// message
	private static $errorCode;	// code

	/**
	* Constructeur de la classe
	*
	* @param void
	* @return void
	*/
	private function
	__construct() {  
	}

	/**
	* getPDO
	* Retrieve a PDO object for a given database
	*
	* @param array Connexion info (database, engine, host, dsn, username, passwd)
	* @return object PDO for this database or false if failed
	*/
	public static function
	getPDO ($args): ?PDO
	{
		// 1st call: create connexion pool
		if (is_null(self::$dbLinks)) {
			self::$dbLinks = array();
		}

// print_r ($args);
		//-------------
		// Get engine
		//-------------
		if (!array_key_exists ('engine', $args))
			throw new Exception ('engine is mandatory');
		$engine = $args['engine'];
		if (!in_array ($engine, [ 'mysql', 'pgsql', 'mssql' ] ))
			throw new Exception ('engine not supported: [' . $engine . ']');

		// mssql is an alias for dblib
		if ($engine == 'mssql')
			$engine = 'dblib';

		// search for this database
		$database = $args['database'];
		if (isset (self::$dbLinks[$database]))
			return self::$dbLinks[$args['database']]['pdo'];

		$dsnItems = [ ];

		if (!($engine == 'dblib' && $args['database'] == '<default>'))
			$dsnItems['dbname'] = $args['database'];

		$dsnItems['host'] = $args['host'];

		if (isset ($args['socket']))
			$dsnItems['unix_socket'] = $args['socket'];

		if (isset ($args['port']))
			$dsnItems['port'] = $args['port'];

		$dsnparts = [ ];
		foreach ($dsnItems as $name => $value)
			$dsnparts[] = $name . '=' . $value;

		$dsn = $engine . ':' . implode (';', $dsnparts);
// echo 'DSN: ' . $dsn . "\n";

		try
		{
			switch ($engine)
			{
			case 'mysql' :
				$pdo = new PDO( $dsn, $args['username'], $args['passwd']);
				break;
			case 'pgsql' :
				$dsn .= ';user=' . $args['username'];
				$dsn .= ';password=' . $args['passwd'];
				$dsn .= ';port=' . 5432;
				$pdo = new PDO($dsn);
				break;
			case 'dblib' :
				$pdo = new PDO( $dsn, $args['username'], $args['passwd']);
				break;
			default :
				throw new Exception ('engine not implemented: ' . $engine);
			}
		}
		catch (PDOException $e)
		{
			self::$errorCode = $e -> getCode();
			self::$lastError = $e -> getMessage();
			return null;
		}
		$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION); // Set Errorhandling to Exception

		self::$dbLinks[$args['database']] = $args;
		self::$dbLinks[$args['database']]['dsn'] = $dsn;
		self::$dbLinks[$args['database']]['pdo'] = $pdo;

		return $pdo;
	}

	/**
	 * Get the message associated with last error encountered
	 * @return string Text of last error
	 */
	public static function
	getLastError (): string
	{
		return self::$lastError;
	}

	/**
	 * Close a connection in the pool identified by its database name
	 * Closing means deleting the pdo object here, there is no guarantee that the link
	 * is physically closed, depends on implementation and configuration
	 * @param string database Name of the database. If non existent, will do nothing.
	 * @return void Nothing
	 */
	public static function
	close (string $database): void
	{
		if (isset (self::$dbLinks[$database]))
		{
			// close database
			self::$dbLinks[$database]['pdo'] = null;

			// clear infos
			unset (self::$dbLinks[$database]);
		}
	}

	/**
	 * Close all connections in the pool
	 */
	public static function
	closeAll (): void
	{
		foreach (self::dbLinks as $database => $dbLink)
			self::close($database);
	}
}

class fwpdo
{
	use errorhandler;

	private $onerror;

	private $host;
	private $database;
	private $transactionCount;

	private $timestart;
	private $time_end;

	public $sql;
	public $success;
	public $nrows;
	public $duration;

	private $errorCode;	// error code
	public $shortmsg;	// error message without SQL statement

	// Read only mode
	private $readOnly;

	// Recording
	private $recording;
	private $sqls;

	private $engine;

	// old syntax: 4 to 8 parms
	// __construct($host, $username, $passwd, $database [,$engine[,$port [,$warn_channel [,$socket]]]])
	// new syntax: 1 array
	// __construct([
	// 'host' =>
	// 'username' => 
	// 'passwd' =>
	// 'database' => mandatory if db is mysql, fac for mssql, pgsql
	// 'socket' => (fac)
	// 'port' => (fac)
	// 'engine' => 'mysql' (default) | 'pgsql' | 'mssql'
	// 'onerror' =>  'raise' | 'die' (default) | 'return'
	//                'exist' alias de 'die'
	// 'warn_channel' => unused, for compatibility
	// ])
	public function
	__construct()
	{
		switch (func_num_args())
		{
		case 4 : // f($host, $username, $passwd, $database)
		case 5 : // f($host, $username, $passwd, $database, $engine)
		case 6 : // f($host, $username, $passwd, $database, $engine, $port)
		case 7 : // f($host, $username, $passwd, $database, $engine, $port, $warn_channel)
		case 8 : // f($host, $username, $passwd, $database, $engine, $port, $warn_channel, $socket)
			$args = self::buildArgsForOldSyntax(func_get_args(),['host','username', 'passwd', 'database', 'engine', 'port', 'warn_channel', 'socket' ] );
			break;

		case 1 : // f([])
			$args = func_get_arg(0);
			if (!is_array ($args))
			{
				$this -> setTemporaryHandler('exception');
				return $this -> errorHandler('Syntax error');
			}
			break;

		default :
			throw new Exception ('fwpdo::construct : unexpected args count: ' . func_num_args() );
		}

		$this -> recording = false;
		$this -> readOnly = false;

		// set default values for unset parms
		$args = self::addMissing($args, [ 'engine' ], 'mysql' );
		if ($args['engine'] == '')
			$args['engine'] = 'mysql';
		$this -> engine = $args['engine'];

		$args = self::addMissing($args, [ 'onerror' ], 'die' );

		// switch to user requested error handler
		$this -> setHandler($args['onerror']);
		// $this -> setTemporaryHandler($args['onerror']);

		if (array_key_exists ('database', $args) && $args['database'] != '')
			$this -> database = $args['database'];
		else
			switch ($this->engine)
			{
			case 'mssql' :
				// une db par defaut est associee a la connexion au niveau de la conf serveur
				// on peut ne pas en definir ici, on a alors cette base par defaut
				// (mais on ignore laquelle)
				$this -> database = $args['database'] = '<default>';	// default database
				break;
			case 'pgsql' :
				// par defaut, la base est 'postgres'
				$this -> database = $args['database'] = 'postgres';
				break;
			case 'mysql' :
				throw new Exception ('missing database');
			}

		$this -> host = $args['host'];
		$this -> pdo = dbCnxPool::getPDO($args);

		// this is likely no transaction has been started
		// but, due to connexion pool, it is not sure
		// if a transaction is started but neither committed or rollback
		// it will remain open
		// (it should be rollbacked)
		$this -> transactionCount = 0;

		if ($this -> pdo == null)
		{
			return $this -> errorHandler(dbCnxPool::getLastError());
		}

		$this -> resetTemporaryHandler();
	}

	static public function
	getVersion(): string
	{
		return '1.11';
	}

	//==================================================
	//
	//             ERROR MANAGEMENT
	//
	//==================================================

	public function
	getLastError($long = true): string
	{
		if ($long)
			return $this -> msg;
		else
			return $this->shortmsg;
	}

	public function
	getErrorCode ()
	{
		return $this -> errorCode;
	}

	//==================================================
	//
	//             FIELD MANAGEMENT
	//
	//==================================================

	/**
	 * Returns a formatted date time field for now
	 * @return string SQL formatted date time
	 */
	public function
	dateTime(): string
	{
		date_default_timezone_set('Europe/Paris');
		return date("Y-m-d H:i:s");
	}

	// format a field value (add quotes, escape etc.)
	// $fielfValue can be null so cannot be types as string
	private function
	formatFieldValue ($fieldValue, bool $trimall = false): string
	{
		if (is_null($fieldValue))
			$str = 'NULL';
		else
		{
			if ($trimall)
				$fieldvalue = trim($fieldvalue);
			$str = $this->quote($fieldValue) ;
		}

		return $str;
	}

	// format a field name
	private function
	formatFieldName (string $fieldName): string
	{
		switch ($this -> engine)
		{
		case 'mysql' :
			return '`' . $fieldName . '`';

		case 'pgsql' :
			return $fieldName;

		case 'mssql' :
			return '[' . $fieldName . ']';
		}
	}

	//==================================================
	//
	//             INFO
	//
	//==================================================

	/**
	 * Returns the number of current database
	 * @return string Name of database
	 */
	public function
	getDatabase(): string
	{
		return $this -> database;
	}

	public function
	getHost(): string
	{
		return $this -> host;
	}

	// for compatibility
	public function get_host() { return $this -> getHost(); }

	public function
	getAttribute (int $attribute)
	{
		return $this -> pdo -> getAttribute($attribute);
	}

	/**
	 * Returns the number of rows read/touched during last call
	 * @return int Number of rows
	 */
	public function
	numRows(): int
	{
		return $this -> nrows;
	}

	/**
	 * Builds the WHERE statement of an SQL query
	 * @param array $fields Array of $fieldname=>$fieldvalue
	 * @return string WHERE part of an SQL statement
	 */
	// 'x > 3'                 ==> WHERE x > 3
	// 'x IS NULL'             ==> WHERE x IS NULL
	// array('x > 3', 'y > 5') ==> WHERE x > 3 AND y > 5
	// array('x' => 3)         ==> WHERE x = 3
	// array('x' => 3, 'y > 5', array('col'=>'z', 'op'=>'<>', 'val'=>'10')
	//                         ==> WHERE x = 3 AND y > 5 AND z <> 10

	private function
	buildWhereStatement ($whereparm, $boolop = 'AND'): string
	{
		return $this -> buildWhereOrHavingStatement($whereparm, 'WHERE', $boolop);
	}
	private function
	buildHavingStatement ($havingparm, $boolop = 'AND'): string
	{
		return $this -> buildWhereOrHavingStatement($havingparm, 'HAVING', $boolop);
	}
	/**
	 * Builds a WHERE or HAVING statement
	 * @param mixed parm WHERE or HAVING parm
	 * @param string statement WHERE or HAVING
	 * @boolop string AND|OR
	 * @return string sql statement
	*/
	private function
	buildWhereOrHavingStatement ($parm, string $statement, $boolop): string
	{
		if ($parm == '')
			return '';

		if (is_array($parm))
			$arr = $parm;
		else
		{
			if ($this -> startsWith ($parm, $statement, false))
				$parm = substr ($parm, strlen($statement));

			$arr = array($parm);
		}
//print_r ($parm);

		$sql = '';
		foreach ($arr as $col=>$item)
		{
			// when array item is named, then $col is a string - is_int() is false
			// when it is not names, $col is a numeric value - is_int() is true
			// is_int (12)  => true
			// is_int('12') => false

			if (is_int ($col))	// no name
			{
				if (is_array ($item))
					$part = $this->formatFieldName($item['col']) . $item['op'] . $this->formatFieldValue($item['value']) ;
				else
					$part = $item;
			}
			else	// named
				// caution: $col must *not* use formatFieldName() here
				if (is_null ($item))
					$part = $col . ' IS NULL';
				else
					if (is_array($item))	// Added 2017-11-25
					{
						$part = '';
						$part .= $col;
						$part .= ' IN ';
						$part .= '(';
						$valuesarray = [ ];
						foreach ($item as $invalue)
							$valuesarray[] = $this->formatFieldValue($invalue);
						$part .= implode (',' , $valuesarray);
						$part .= ')';
					}
					else
						$part = $col . '=' . $this->formatFieldValue($item);

			if ($sql == '')
				$sql .= " \n" . $statement . ' ';
			else
				$sql .= "\n  ".$boolop." ";

			$sql .= $part;
		}

		return $sql;
	}

	private function
	startSql ($sql)
	{
		// Reset infos from prev request
		$this -> msg = '';
		$this -> shortmsg = '';
		$this -> nrows = 0;
		$this -> duration = 0;

		$this -> sql = $sql;
		$this -> time_start = microtime(true);
		$this -> success = false;
	}

	private function
	stopSql ()
	{
		$this -> time_end   = microtime(true);
		$this -> duration   = $this -> time_end - $this -> time_start;
		$this -> record();
	}

	private function
	record ()
	{
return;
		date_default_timezone_set('Europe/Paris');
		$str = date('Y-m-d H:i:s');
		$str .= ' SQL=[' . $this -> sql . ']';
		$str .= ' duration=[' . sprintf('%.2f', $this -> duration) . ' ms]';
		$str .= ' rows=' . $this->nrows;
		$str = str_replace ("\n", ' ', $str);

		$fp = @fopen ('/tmp/sql.log', 'a');
		if ($fp)
		{
			fwrite ($fp, $str . "\n");
			fwrite ($fp, "---------------------------------------------------------------------------\n");
			fclose ($fp);
		}
	}

	/**
	 * Sets DB engine charset
	 * @param string name of charset
	 * @return bool true if success, false if failure
	 */
	public function
	setCharset (string $charset)
	{
		// [1.8] mysql recognizes 'utf8', not 'utf-8'
		$charset = strtolower ($charset);
		if ($charset == 'utf-8')
			$charset = 'utf8';

		return $this -> executeSql ('SET NAMES ' . $charset);
	}

	/**
	 * Returns charset of current database
	 * @return string charset
	 */
	public function
	getCharset(): string
	{
		$row = $this -> select1 ([ 'select' => "@@character_set_database AS C", 'from' => null ]);
		return (($row == null) ? '' : $row['C']);
	}

	/**
	 * Returns collation of current database
	 * @return string collaction
	 */
	public function
	getCollation(): string
	{
		$row = $this -> select1 ([ 'select' => "@@collation_database AS C", 'from' => null ]);
		return (($row == null) ? '' : $row['C']);
	}

	// if success, returns true
	// if failure, behaviour depends on $onerror parm
	//             if $onerror parm unset, then based on $this->onerror
	//             'die' -> write a message on stdout and die
	//             'return' -> return false
	//             'raise','exception' -> raise an exception
	//             'user' -> call function defined by setHandler() and return its return value
	// forceExec: if true executes statement, overriding read-only mode
	private function
	executeSql (string $sql, string $onerror = '', bool $forceExec = false)
	{
		if ($this -> recording)
			$this -> doRecord ($sql);

		if ($forceExec || !$this -> readOnly)
		{
			$this -> startSql ($sql);
			$this -> success = false;
			try
			{
				$this -> nrows = $this->pdo->exec($sql);
				$this -> success = true;
			}
			catch (PDOException $e)
			{
				$this->errorCode = $e -> getCode();
				$this->shortmsg = $e -> getMessage();
				$this->msg = $sql . ': ' . $e -> getMessage();
			}
			// finally
			$this -> stopSql ();

			if ($this -> success)
				return true;

			$this -> setTemporaryHandler ($onerror);
			return $this -> errorHandler ($this->msg);
		}
		else
			$this -> doRecord ($sql);
	}

	private function
	must_executeSql (string $sql)
	{
		$this -> executeSql($sql, 'die');
	}

	private function
	fetch (string $sql, string $onerror='')
	{
		$this -> startSql ($sql);
		try
		{
			$this -> success = false;
			$st = $this->pdo->query($sql); // returns a PDOStatement
			$this->nrows = $st -> rowCount();
			$arr = array();
/*
			while (($row = $st -> fetch(PDO::FETCH_ASSOC)))
				$arr[] = $row;
*/
$arr = $st -> fetchAll(PDO::FETCH_ASSOC);
			$this -> success = true;
		}
		catch (PDOException $e)
		{
			$this->errorCode = $e -> getCode();
			$this->shortmsg = $e -> getMessage();
			$this->msg = $sql . ': ' . $e -> getMessage();
			$arr = '';
		}
		// finally
		$this -> stopSql ();

		if ($this -> success)
			return $arr;

		$this -> setTemporaryHandler ($onerror);
		return $this -> errorHandler ($this->msg);
	}

	private function
	buildArgsForOldSyntax (array $oldArgs, array $parms): array
	{
		$num_args = count ($oldArgs);
		if ($num_args > count ($oldArgs))
			return $this -> errorHandler ('too many parameters', 1);

		$newArgs = array();
		$argi = 0;
		foreach ($parms as $parm)
			if ($argi < $num_args)
				$newArgs[$parm] = $oldArgs[$argi++];
			else
				$newArgs[$parm] = '';

		return $newArgs;
	}

	//--------------------------------------------------------------------
	//                            COUNT
	//--------------------------------------------------------------------

	public function
	count ($fromparm, $whereparm = '', $joinparm = '', $onerror = ''): int
	{
// tracelog ('[fwpdo::count] onError: [' . $onerror . ']');
		$this -> setTemporaryHandler ($onerror);

		if (is_array ($fromparm))	// new syntax
			$args = $fromparm;
		else
		{
			$args = array();
			$args['where'] = $whereparm;
			$args['from']  = $fromparm;
			$args['join']  = $joinparm;
			$args['onerror'] = $onerror;
			$args['boolop']  = 'AND';
		}
		$args['select'] = 'COUNT(*) NB';
		$args['limit']   = '';
		$args['orderby'] = '';
		$args['order']   = '';

		$rows = $this -> select ($args);
//tracelog ('[fwpdo::count] ' . $this->sql);
		if (!$rows || $rows == '' || count($rows) < 1)
			return $this -> errorHandler ('No result');
		else
		{
			$this -> resetTemporaryHandler();
			return $rows[0]['NB'];
		}
	}

	public function
	must_count ($fromparm, $whereparm = '', $joinparm = ''): int
	{
		return $this -> count ($fromparm, $whereparm, $joinparm, 'die');
	}

	//--------------------------------------------------------------------
	//                         DESCRIBE / SHOW COLUMNS
	//--------------------------------------------------------------------

	// describe ('table')
	// describe ([ 'from' => 'table', 'where' => [ ... ] );
	public function
	describe ()
	{
		$num_args = func_num_args();

		if ($num_args  != 1)
			return $this -> errorHandler ('fwpdo::describe() expects 1 parameter', 1);

		$args = func_get_arg(0);
		if (!is_array( $args ))
			$args = [ 'from' => $args ];
		$args = $this -> addMissing ($args, [ 'where' ], '' );
		$args = $this -> addMissing ($args, [ 'boolop' ], 'AND' );

		if (!array_key_exists ('from', $args) || $args['from'] == '')
			return $this -> errorHandler ('fwpdo::describe() missing table', 1);

		$whereStatement   = $this -> buildWhereStatement ($args['where'], $args['boolop']);
		$fromStatement    = $args['from'];
		$sql = $this -> concatStrings(  'SHOW COLUMNS FROM', $fromStatement, $whereStatement );

		if ($sql == '')
			return $this -> errorHandler ('fwpdo::describe() unable to build SQL statement', 1);

		$rows = $this->fetch ($sql);

		return $rows;
	}

	//--------------------------------------------------------------------
	//                            SELECT
	//--------------------------------------------------------------------

	// builds args array (new style) when called with 9 parms (old style)
	private function
	buildArgsForOldSelectSyntax ($oldArgs): array
	{
		$parms = [  'select', 'from', 'where', 'orderby', 'limit', 'join', 'order', 'groupby', 'onerror' ];
		return $this -> buildArgsForOldSyntax ($oldArgs, $parms);
	}

	public function
	select1 (array $pdoparms): ?array
	{
		$rows = $this -> select ($pdoparms);
		if ($this -> num_rows () == 0)
			return null;
		if ($this -> num_rows() > 1)
			throw new Exception ('select1: mutiple results');
		return $rows[0];
	}

	// select() accepts various syntaxes and argument types
	// 1- an array
	// select( [ 'select' => '*', 'from' => 'table', 'where' => ['field'=>1] ] )
	// 2- a string
	// select ('SELECT * FROM table WHERE field=1')
	// 3- a list of arguments (deprecated)
	// select ('*', 'table', [ 'field' => 1 ]);

	public function
	select ()
	{
		$num_args = func_num_args();

		if ($num_args  == 0)
			return $this -> errorHandler ('fwpdo::select() expects 1 parameter', 1);

		if ($num_args  == 1)	// new syntax
		{
			$args = func_get_arg(0);
			if (is_array( $args ))
				$sql = $this -> buildSelectRequest($args);
			else
				$sql = $args;
		}
		else // old syntax
		{
			$args = $this -> buildArgsForOldSelectSyntax(func_get_args() );
			if ($args == '')
				return $this -> errorHandler ('fwpdo::select() bad parameters', 1);
			$sql = $this -> buildSelectRequest($args);
		}

		if ($sql == '')
			return '';

		$rows = $this->fetch ($sql);
		return $rows;
	}

	public function
	select_sql (string $sql): array
	{
		$result = $this -> fetch ($sql);
		if (is_array($result))
			return $result;
		throw new Exception ('fwpdo::select_sql failed for ' . $sql . ': ' . $this->shortmsg);
	}

	// for compatibility only
	public function
	must_select ($selectparm, $fromparm, $whereparm='', $orderItems="", $limitstr="", $joinparm = '', $sortorder = '', $groupparm = '')
	{
		return $this->select($selectparm, $fromparm, $whereparm, $orderItems, $limitstr, $joinparm, $sortorder, $groupparm, 'die');
	}

	// for compatibility only
	public function
	must_select_sql ($sql)
	{
		return $this -> fetch ($sql, 'die');
	}

	/**
	 * Builds a SELECT statement based on parameters
	 * @param array $args Parms
	 * @return string SQL statement for reading (SELECT)
	 */
	private function
	buildSelectRequest ($args): string
	{
		// Create missing entries, if any

		$args = $this -> addMissing ($args, [ 'select' ], '*' );
		$args = $this -> addMissing ($args, [ 'join', 'where', 'groupby', 'orderby', 'order', 'limit', 'having' ] );
		$args = $this -> addMissing ($args, [ 'from' ], 'MISSING' );
		$args = $this -> addMissing ($args, [ 'boolop' ], 'AND' );
//tracelog ('[buildSelectRequest] ' . tracelog_dump($args));

		//---------------------------------
		// build SQL parts
		//---------------------------------

		$selectStatement  = $this -> buildSelectStatement ($args['select']);
		// not all select requests have a FROM statement
		// for instance, SELECT COLLATION('foo') has no FROM
		// to distinguish lack of FROM from an error,
		// it is required that requests with no FROM set the parm to NULL
		// otherwise, an exception (missing from) will be raised
		if ($args['from'] == 'MISSING')
			return $this -> errorHandler ('fwpdo::select() Missing FROM');
		if ($args['from'] == null) // legal no 'from'
			$fromStatement    = '';
		else	// non-empty 'from' MUST be provided
			$fromStatement    = $this -> buildFromStatement ($args['from']);
		$joinStatements   = $this -> buildJoinStatements ($args['join']);
		$whereStatement   = $this -> buildWhereStatement ($args['where'], $args['boolop']);
		$havingStatement  = $this -> buildHavingStatement ($args['having']);
		$groupByStatement = $this -> buildGroupByStatement ($args['groupby']);
		$orderByStatement = $this -> buildOrderByStatement($args['orderby']);
		$orderStatement   = $this -> buildOrderStatement($args['order']);
		$limitStatement   = $this -> buildLimitStatement ($args['limit'], 2);

		//---------------------------------
		// check mandatory ones
		//---------------------------------

		// if ($fromStatement == '')
			// return $this -> errorHandler ('fwpdo::select() Empty FROM', 1);

		if ($selectStatement == '')
			return $this -> errorHandler ('fwpdo::select() Empty SELECT', 1);

		// Canot use ASC or DESC when no ORDER set
		if ($orderStatement != '' && $orderByStatement == '')
			$orderStatement = '';

		//---------------------------------
		// Altogether
		//---------------------------------

		$sql = $this -> concatStrings(  
			$selectStatement,
			$fromStatement,
			$joinStatements,
			$whereStatement,
			$havingStatement,
			$groupByStatement,
			$orderByStatement,
			$orderStatement,
			$limitStatement  );

		//---------------------------------
		// Return result
		//---------------------------------

		return $sql;
	}

	private function
	buildOrderStatement (string $orderParm): string
	{
		$orderStatement = strtoupper($orderParm);

		return $orderStatement;
	}

	private function
	buildSelectStatement ($selectParm): string
	{
		if (is_array ($selectParm))
			$selectParts = $selectParm;
		else
			$selectParts = array($selectParm);

		$selectStatement = '';
		foreach ($selectParts as $column)
		{
			if ($selectStatement == '')
				$selectStatement .= 'SELECT ';
			else
				$selectStatement .= ', ';
			$selectStatement .= $column;
		}
		return $selectStatement;
	}

	private function
	buildTablesList ($tablesParm): string
	{
		if (is_array ($tablesParm))
			$tables = implode (',' , $tablesParm );
		else
			$tables = $tablesParm;

		if ($tables == '')
			throw new Exception ('No table provided');

//echo '['.$tables.']';
		return $tables;
	}

	private function
	buildFromStatement ($fromParm): string
	{
		$tables = $this -> buildTablesList($fromParm);
		if ($tables != '')
			$fromStatement = $this -> concatStrings ('FROM', $tables);
		else
			$fromStatement = '';

		return $fromStatement;
	}

	private function
	buildJoinStatements ($joinParm): string
	{
		return $this -> concatStrings ($joinParm);
	}

	private function
	buildOrderByStatement($orderParm): string
	{
		if (is_array($orderParm))
			$orderItems = $orderParm;
		else
			$orderItems = array($orderParm);

		$orderStatement = '';
		foreach ($orderItems as $orderItem)
		{
			if ($orderItem == '')
				continue;
			if ($orderStatement == "")
				$orderStatement .= " \nORDER BY ";
			else
				$orderStatement .= ", \n";
			$orderStatement .= $orderItem;
		}
		return $orderStatement;
	}

	private function
	buildGroupByStatement ($groupparm): string
	{
		if (!is_array($groupparm))
			$grouparr = array($groupparm);
		else
			$grouparr = $groupparm;

		$groupByStatement = '';
		foreach ($grouparr as $grouppart)
		{
			if ($grouppart == '')
				continue;

			if ($groupByStatement == '')
				$groupByStatement .= " \nGROUP BY ";
			else
				$groupByStatement .= ", \n";
			$groupByStatement .= $grouppart;
		}

		return $groupByStatement;
	}

	// 1- max 10 records
	// > buildLimitStatement('10')
	// > buildLimitStatement('limit 10')
	// > buildLimitStatement([ 10 ])
	// mysql => 'LIMIT 10'
	// mssql => 'FETCH NEXT 10 ROWS ONLY'
	// pgsql => 'FETCH NEXT 10 ROWS ONLY'
	//
	// 2- max 10 records, starting at 50th
	// > buildLimitStatement('50,10')
	// > buildLimitStatement('limit 50,10')
	// > buildLimitStatement([ 50, 10 ])
	// mysql => 'LIMIT 50,10' ou 'LIMIT 10 OFFSET 50'
	// mssql => 'OFFSET 50 ROWS FETCH NEXT 10 ROWS ONLY'
	// pgsql => 'OFFSET 50 FETCH NEXT 10 ROWS ONLY'

	private function
	buildLimitStatement ($limitParm, int $maxParts): string
	{
		if (!is_array ($limitParm))
		{
			$limitParm = strtoupper ($limitParm);
			$limitParm = str_replace ('LIMIT', '', $limitParm);
			$limitParm = str_replace (' ', '', $limitParm);

			$limitParm = explode (',' , $limitParm);
		}

		if ($maxParts == 2 && count($limitParm) == 2)	// offset,max
		{
			$offset  = (int)$limitParm[0];
			$maxrows = (int)$limitParm[1];
		}
		else	// max
		{
			$offset  = 0;
			$maxrows = (int)$limitParm[0];
		}

		if ($maxrows == 0)
			return '';

		switch ($this -> engine)
		{
		case 'mysql' : return $this->buildLimitStatementForMysql($offset, $maxrows);
		case 'pgsql' : return $this->buildLimitStatementForPgsql($offset, $maxrows);
		case 'mssql' : return $this->buildLimitStatementForMSSql($offset, $maxrows);
		}
	}

	// NB: works with SQL Server 2012 and beyond
	// NB: requires ORDER BY clause
	private function
	buildLimitStatementForMSSql(int $offset, int $maxrows): string
	{
		$limitStatement .= 'OFFSET ' . $offset . ' ROWS';
		$limitStatement .= ' ';
		$limitStatement .= 'FETCH NEXT ' . $maxrows . ' ROWS ONLY';

		return $limitStatement;
	}

	private function
	buildLimitStatementForPgsql(int $offset, int $maxrows): string
	{
		$limitStatement .= 'OFFSET ' . $offset;
		$limitStatement .= ' ';
		$limitStatement .= 'FETCH NEXT ' . $maxrows . ' ROWS ONLY';

		return $limitStatement;
	}

	private function
	buildLimitStatementForMysql(int $offset, int $maxrows): string
	{
		if ($offset == 0)
			$limitStatement = ' LIMIT ' . $maxrows;
		else
			$limitStatement = ' LIMIT ' . $offset . ',' . $maxrows;

		return $limitStatement;
	}

	//--------------------------------------------------------------------
	//                            DELETE
	//--------------------------------------------------------------------

	/**
	 * Issue a DELETE request (old syntax)
	 * @param mixed $from
	 * @param mixed $where
	 * @param mixed $limit
	 * @param mixed $onerror
	 * @return bool true if success, calls errorhandler if fails
	 */

	/**
	 * Issue a DELETE request (new syntax)
	 * @param array $parms (from, where, limit, onerror)
	 * @return bool true if success, calls errorhandler if fails
	 *
	 * caution: if no record is found (so no delete occurs),
	 * the method will be considered as ok
	 * It is the same as a SELECT reading no record
	 * Only bad requests (missing table, column etc.) will be considered
	 * as an error.
	 */
	public function
	delete ()
	{
		$num_args = func_num_args();

		$opts = [  'from', 'where', 'limit', 'onerror' ] ;

		if ($num_args  == 1)	// new syntax
			$args = $this -> addMissing (func_get_arg(0), $opts);
		else // old syntax
			$args = $this -> buildArgsForOldSyntax(func_get_args(), $opts);

		$this -> setTemporaryHandler ($args['onerror']);

		$sql = $this -> buildDeleteRequest ($args);
		if ($sql == '')
			return $this -> errorHandler('Failed to build SQL statement',1,$args['onerror']);

		$cr = $this -> executeSql ($sql);
		$this -> resetTemporaryHandler();
		return $cr;
	}

	// this is a new function, it does not implements compatibility mode with multiple args
	// if it cannot delete 1 record (no record matches where clause for example), it will return an error
	// this differs from the behaviour of delete()
	public function
	delete1(array $args): bool
	{
		$opts = [  'from', 'where', 'onerror' ] ;
		$args['limit'] = 1;
		$args = $this -> addMissing ($args, $opts);

		$this -> delete($args);
		if ($this -> nrows != 1)
		{
			$this -> success = false;
			return $this -> errorHandler('delete: no record found',1,$args['onerror']);
		}
		return true;
	}

	// shorthand
	public function
	deleteOrDie ($table, $whereset)
	{
		$this -> delete ($table, $whereset, '', 'die');
	}

	/**
	 * @return bool true if success, false if failure
	 */
	public function
	deleteSql (array $sql, string $onerror = '')
	{
		return $this -> executeSql ($sql, $onerror);
	}

	public function
	must_delete_sql (array $sql)
	{
		return $this -> delete_sql ($sql, 'die');
	}

	private function
	buildDeleteRequest (array $args): string
	{
		$fromStatement    = $this -> buildFromStatement ($args['from']);
		$whereStatement = $this -> buildWhereStatement ($args['where']);
		$limitStatement  = $this -> buildLimitStatement ($args['limit'], 1);

		$sql = $this -> concatStrings ( 'DELETE', $fromStatement, $whereStatement , $limitStatement );

		return $sql;
	}

	//--------------------------------------------------------------------
	//                      SIMULATION AU LIEU D'EXECUTION
	//--------------------------------------------------------------------

	public function
	setReadOnly (bool $truefalse = true): void
	{
		$this -> readOnly = $truefalse;
	}

	//--------------------------------------------------------------------
	//                      ENREGISTREMENT DES REQUETES
	//--------------------------------------------------------------------

	public function
	setRecording (bool $truefalse = true): void
	{
		$this -> recording = true;
		$this -> resetRecorder();
	}

	public function
	doRecord (string $sql): void
	{
		$this -> sqls[] = $sql;
	}

	public function
	getRecorded(): array
	{
		return $this -> sqls;
	}

	private function
	resetRecorder (): void
	{
		$this -> sqls = array();
	}

	//--------------------------------------------------------------------
	//                            TRANSACTIONS
	//--------------------------------------------------------------------

	// the class will prevent from using nested transactions
	// with a transaction count
	// beginTransaction increases transaction count
	// commit and rollback decreases the count
	// real beginTransaction / commit / rollback orders 
	// are only sent for outmost transaction,
	// i.e. when transaction count moves from 0 to 1 for START
	// and 1 to 0 for commit/rollback

	// in read-only mode, transactions are meaningless and not issued

	// for debug purposes
	public function
	getTransactionCount (): int
	{
		return $this -> transactionCount;
	}

	public function
	beginTransaction(): void
	{
		if ($this -> readOnly)
			return;

		$this -> transactionCount++;
		if ($this -> transactionCount == 1)
		{
			$this -> pdo -> beginTransaction();
			$this -> sql = 'START TRANSACTION';
		}
	}

	// alias
	public function beginTrans() { return $this -> beginTransaction(); }

	/**
	 * @return bool true if ok, false if not
	 */
	public function
	commit(): bool
	{
//tracelog ('[fwpdo::commit] commit - transactionCount='.$this->transactionCount);
		if ($this -> readOnly)
			return true;

		if ($this -> transactionCount > 1)
		{
			$this -> transactionCount--;
			return true;
		}

		if ($this -> transactionCount == 1)
		{
			$this -> sql = 'COMMIT';
			try
			{
				if (!$this -> pdo -> commit())
					return false;
//tracelog ('[fwpdo::commit] commit successful');
			}
			catch (PDOException $e)
			{
				return false;
			}
			$this -> transactionCount = 0;
			return true;
		}

		// if transactionCount == 0, no transaction has been started
		return false;
	}

	public function
	rollback(): bool
	{
		if ($this -> readOnly)
			return true;

		if ($this -> transactionCount > 1)
		{
			$this -> transactionCount--;
			return true;
		}

		if ($this -> transactionCount == 1)
		{
			$this -> sql = 'ROLLBACK';
			try
			{
				$this -> pdo -> rollback();
			}
			catch (PDOException $e)
			{
				return false;
			}

			$this -> transactionCount = 0;
			return true;
		}

		// if transactionCount == 0, no transaction has been started
		return false;
	}

	//--------------------------------------------------------------------
	//                            UPDATE
	//--------------------------------------------------------------------

	/**
	 * Update one or more records
	 * Limitation: WHERE clause can only contain '=' operators
	 * @param string $table Table to update
	 * @param array $fields Array of fields (name=>value)
	 * @param array $whereset Array of name=>value to build a WHERE clause with '='
	 * @param boolean $trimall IF true, will trim fields (default: false)
	 * @return boolean True if sucessful, false is failed
	 */
	public function
	update ()
	{
		$parms = [ 'from', 'fields', 'where', 'trimall', 'onerror', 'limit', 'join' ];

		$num_args = func_num_args();

		if ($num_args  == 1)	// new syntax
			$args = $this -> addMissing (func_get_arg(0), $parms);
		else // old syntax
			$args = $this -> buildArgsForOldSyntax (func_get_args(), $parms);

		$this -> setTemporaryHandler ($args['onerror']);

		$sql = $this -> buildUpdateRequest ($args);
		if ($sql == '')
			return $this -> errorHandler('Empty SQL');

		$cr = $this -> executeSql ($sql);

		$this -> resetTemporaryHandler();
		return $cr;
	}

	public function
	must_update (string $table, array $fields, $whereset, bool $trimall=false)
	{
		return $this -> update ($table, $fields, $whereset, $trimall, 'die');
	}

	/**
	 * Issue an UPDATE statement
	 * @param string $sql SQL statement
	 * @param boolean $trimall IF true, will trim fields (default: false)
	 * @return boolean True if sucessful, false is failed
	 */
	public function
	update_sql (string $sql, bool $trimall = false, string $onerror = '')
	{
		return $this -> executeSql ($sql, $onerror);
	}

	public function
	must_update_sql (string $sql, bool $trimall = false)
	{
		return $this -> update_sql ($sql, $trimall, 'die');
	}

	/**
	 * Builds an UPDATE statement
	 * @param string $table Dataabase table
	 * @param array $fields Array of $fieldname=>$fieldvalue
	 * @param array $whereset Array of $fieldname=>$fieldvalue
	 * @return string SQL statement
	 */
	private function
	buildUpdateRequest (array $args): string
	{
		// Create missing entries, if any

		$tables  = $this -> buildTablesList ($args['from']);
		$whereStatement = $this -> buildWhereStatement ($args['where']);
		$setStatement   = $this -> buildSetStatement ($args['fields'], $args['trimall']);
		$limitStatement   = $this -> buildLimitStatement ($args['limit'], 1);
		$joinStatements   = $this -> buildJoinStatements ($args['join']);

		$sql = $this -> concatStrings ( 
			'UPDATE',
			$tables,
			$joinStatements,
			'SET',
			$setStatement,
			$whereStatement,
			$limitStatement
			 );
		return $sql;
	}

	private function
	buildSetStatement (array $setFields, bool $trimall = false): string
	{
		$setArray = array();

		foreach ($setFields as $fieldName=>$fieldValue)
			$setArray[] =  $this->formatFieldName($fieldName) . '=' . $this->formatFieldValue($fieldValue, $trimall);

		return implode (',' , $setArray);
	}

	//--------------------------------------------------------------------
	//                            INSERT
	//--------------------------------------------------------------------

	/**
	 * Insert a single record
	 * @param string $table Database table
	 * @param array $fields Array of columns (name=>value)
	 * @param boolean $trimall Trim before insert if true (default: no)
	 * @return int ID of inserted element, 0 if failed
	 */
	public function
	insert (): int
	{
		$num_args = func_num_args();

		if ($num_args  == 1)	// new syntax
			$args = func_get_arg(0);
		else // old syntax
			$args = $this -> buildArgsForOldInsertSyntax(func_get_args());

		$sql = $this -> buildInsertRequest ($args);
		
		if (isset ($args['onerror']))
			$onerror = $args['onerror'];
		else
			$onerror = '';
		return $this -> insert_sql ($sql, $onerror);
	}

	private function
	buildArgsForOldInsertSyntax (array $args): array
	{
		$parms = array();
		$parms[] = 'from';
		$parms[] = 'fields';
		$parms[] = 'trimall';
		$parms[] = 'onerror' ;
		return $this -> buildArgsForOldSyntax($args, $parms);
	}

	public function
	must_insert (string $table, array $fields, bool $trimall=false)
	{
		return $this -> insert ($table, $fields, $trimall, 'die');
	}

	/**
	 * Insert a single record from an SQL statement
	 * @param string $sql SQL statement
	 * @return int ID of inserted element, 0 if failed
	 */
	public function
	insert_sql (string $sql, string $onerror = ''): int
	{
		if (!$this -> executeSql($sql, $onerror))
			return 0;

		return $this -> lastInsertId();
	}

	public function
	must_insert_sql (string $sql)
	{
		return $this -> insert_sql ($sql, 'die');
	}

	/**
	 * Builds and INSERT statement
	 * @param string $table Database table
	 * @param array $fields Array of $fieldname=>$fieldvalue
	 * @param bool $trimall TRUE if fields should be trim'ed
	 * @return string
	 */
	private function
	buildInsertRequest ($args): string
	{
		// Create missing entries, if any

		$args = $this -> addMissing ($args, [ 'from', 'fields' ] );

		$tables  = $this -> buildTablesList ($args['from']);
		$fieldNames = $this -> buildFieldNamesList ($args['fields']);
		if (isset ($args['trimall']))
			$trimall = $args['trimall'];
		else
			$trimall = false;
		$fieldValues = $this -> buildFieldValuesList ($args['fields'], $trimall);
		$sql = $this -> concatStrings ('INSERT', 'INTO', $tables, $fieldNames, 'VALUES', $fieldValues);
		return $sql;
	}
	private function
	buildFieldNamesList (array $fields): string
	{
		$fieldNameArray = array();
		foreach ($fields as $fieldname => $fieldvalue)
			$fieldNameArray[] = $this -> formatFieldName ($fieldname);

		return '(' . implode (',' , $fieldNameArray) . ')';
	}
	private function
	buildFieldValuesList (array $fields, bool $trimall): string
	{
		$fieldValueArray = array();
		foreach ($fields as $fieldname => $fieldvalue)
			$fieldValueArray[] = $this->formatFieldValue($fieldvalue, $trimall);

		return '(' . implode(',' , $fieldValueArray) . ')';
	}

	//==================================================
	//
	//             PDO METHODS
	//
	//==================================================

	// if no INSERT has been performed, will return 0
	public function
	lastInsertId (string $name = null): int
	{
		return $this -> pdo -> lastInsertId ($name);
	}

	public function
	quote ($s): string
	{
		switch (gettype($s))
		{
		case 'string':
		case 'integer':
		case 'double':
			return $this -> pdo -> quote ($s);

		case 'bool' :
		case 'boolean' :
			return ($s ? 'TRUE' : 'FALSE');

		default: 
			//tracelog ('quote(): parm is expected as string, '.gettype($s).' provided: '.tracelog_dump($s));
			return $s;
		}
	}

	//==================================================
	//
	//             MISC UTILITIES
	//
	//==================================================

	private function
	startsWith (string $str, string $pattern, bool $case_sensitive = true): bool
	{
		if ($case_sensitive)
			return ( substr ($str, 0, strlen($pattern)) == $pattern );
		else
			return ( strcasecmp (substr ($str, 0, strlen($pattern)) , $pattern ) == 0);
	}

	// concatenate strings provided
	// args can mix strings and arrays of strings
	private function
	concatStrings (): string
	{
		$parms = func_get_args();

		$result = '';

		foreach ($parms as $parm)
		{
			if (is_array ($parm))
				$result .= implode (' ', $parm);
			else
				$result .= $parm;
			$result .= ' ';
		}

		return trim ($result);
	}

	/**
	 * Returns an array with all keys, creating missing ones if mecessary
	 * @param array Array to fill
	 * @param array entries Keys
	 * @param mixed (optional) Value to use for entries created - default value: empty string
	 * @return array Filled array
	 */
	protected function
	addMissing (array $args, array $entries, string $defaultValue = ''): array
	{
		foreach ($entries as $entry)
			if (!array_key_exists($entry, $args))
				$args[$entry] = $defaultValue;

		return $args;
	}

	// old names for compatibility only

	public function on_error ($expected_behaviour) { return $this -> onError($expected_behaviour); }
	public function get_last_error () { return $this -> getLastError(); }
	//public function datetime () { return $this -> dateTime(); }
	public function get_database () { return $this -> getDatabase(); }
	public function num_rows () { return $this -> numRows(); }
	public function set_charset ($charset) { return $this -> setCharset($charset); }
	public function must_delete ($table, $whereset) { return $this -> deleteOrDie($table, $whereset); }
	public function delete_sql ($sql, $onerror = '') { return $this -> deleteSql ($sql, $onerror); }
	public function setwarnchannel($channel) { }

	public function
	sql_build_select ($selectarr, $fromarr, $wherearr, $orderarr, $limitstr,$joinarr, $sortorder, $grouparr, $havingarr)
	{
		$parms = [  'select', 'from', 'where', 'orderby', 'limit', 'join', 'order', 'groupby', 'having' ];
		$newArgs = $this -> buildArgsForOldSyntax (func_get_args(), $parms);
		$sql = $this -> buildSelectRequest ($newArgs);
		return $sql;
	}
}
?>
