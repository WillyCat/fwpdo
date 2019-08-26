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
2019-01-18  1.12  FHO  executeSql: was private, now public
                       added comments
2019-02-03  1.13  FHO  - buildSelectRequest now public
                       - parameter $onerror is no longer used in any method
                       for compatibility reasons, it is still accepted but ignored
                       to set a handler, either
                       1- do nothing
                       fwpdo will raise an exception when error occurs
                       2- set a permanent handler
                       call $pdo->setHandler() to change fwpdo behaviour
                       options are: die, return, raise
                       'die' means call die()
                       'return' means return a special value (like old fashioned functions)
                       'raise' means trigger an exception
                       3- set a temporary handler
                       call $pdo->setTemporaryHandler()
                       same options as setHandler()
                       handler will override permanent handler but only for next error
                       when error occurs, fwpdo will behave according to temporary handler and return to permanent handler
                       can be reset by calling $pdo->resetTemporaryHandler()
2019-02-04  1.14  FHO  Added some comments
                       Cleaned code for $onerror handling
2019-03-05  1.15  FHO  Upon failure, methods commit(), rollback() returned
                       FALSE, no matter setting of current error handler
                       buildWhereStatement now public (used in ol_sql.inc)
2019-04-02  1.16  FHO  - buildWhereStatement now accepts [ 'col' => [ 'op' => '<>', 'value' => 12 ] ]
                       result is same as [ [ 'col' => 'col', 'op' => '<>', 'value' => 12 ] ]
                       i.e. " WHERE col <> 12 "
                       - 'x' => [] used to generate "WHERE x IN ()" now generates nothing
2019-04-18  1.17  FHO  getPDO now accepts 'persistent' key (true/false) to parameter persistent/non-persistent db connection - default is non-persistent, consistent with previous versions
2019-05-13  1.18  FHO  removed support for multiple error handlers
                       all errors raise exceptions
                       deprecated: setHandler, getHandler, resetHandler,
                       errorHandler,
                       setTemporaryHandler, resetTemporaryHandler
                       various code cleaning: removed all references to onerror
2019-08-20  1.19  FHO  new: use(), prepare()

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

class fwpdoException extends Exception
{
}

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
		$this -> persistent = false;
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

		$opts = [ ];

		if (array_key_exists ('persistent', $args) && $args['persistent'])
			$opts[PDO::ATTR_PERSISTENT] = true; // persistent connections

		try
		{
			switch ($engine)
			{
			case 'mysql' :
				$pdo = new PDO( $dsn, $args['username'], $args['passwd'], $opts);
				break;
			case 'pgsql' :
				$dsn .= ';user=' . $args['username'];
				$dsn .= ';password=' . $args['passwd'];
				$dsn .= ';port=' . 5432;
				$pdo = new PDO($dsn, null, null, $opts);
				break;
			case 'dblib' :
				$pdo = new PDO( $dsn, $args['username'], $args['passwd'], $opts);
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
	// 'socket' => (opt)
	// 'port' => (opt)
	// 'engine' => 'mysql' (default) | 'pgsql' | 'mssql'
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
			$args = self::buildArgsFromOldSyntax(func_get_args(),['host','username', 'passwd', 'database', 'engine', 'port', 'warn_channel', 'socket' ] );
			break;

		case 1 : // f([])
			$args = func_get_arg(0);
			if (!is_array ($args))
				throw new fwpdoException('Syntax error');
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
			throw new fwpdoException(dbCnxPool::getLastError());
	}

	static public function
	getVersion(): string
	{
		return '1.19';
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
	 * Returns the name of current database
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
	// 'x > 3'                  ==> WHERE x > 3
	// 'x IS NULL'              ==> WHERE x IS NULL
	// array('x > 3', 'y > 5')  ==> WHERE x > 3 AND y > 5
	// array('x' => 3)          ==> WHERE x = 3
	// array('x' => 3, 'y > 5', 'x' => [ 'op'=>'<>', 'val'=>'10' ]
	//                          ==> WHERE x = 3 AND y > 5 AND z <> 10
	// 'x' => [ 'a', 'b' ]      ==> WHERE x IN ('a', 'b')
	// 'x' => [ 'op'=>'>', 12 ] ==> WHERE x > 12

	function
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
				{
					if (is_array($item))	// Added 2017-11-25
					{
						$part = '';
						if (array_key_exists ('op', $item))
						{
							$part = $this->formatFieldName($col) . $item['op'] . $this->formatFieldValue($item['value']) ;
						}
						else
							if (count ($item) > 0) // added 2019-04-02 to avoid => WHERE col IN ()
							{
								$part .= $col;
								$part .= ' IN ';
								$part .= '(';
								$valuesarray = [ ];
								foreach ($item as $invalue)
									$valuesarray[] = $this->formatFieldValue($invalue);
								$part .= implode (',' , $valuesarray);
								$part .= ')';
							}
					}
					else
						$part = $col . '=' . $this->formatFieldValue($item);
				}

			if ($part != '')
			{
				if ($sql == '')
					$sql .= " \n" . $statement . ' ';
				else
					$sql .= "\n  ".$boolop." ";
			}

			$sql .= $part;
		}

		return $sql;
	}

	/**
	 * Notify of the execution of an SQL statement
	 * @parm string $sql SQL statement
	 */
	private function
	startSql (string $sql): void
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

	/**
	 * Notify that the SQL statement is now over (success or failed)
	 */
	private function
	stopSql (): void
	{
		$this -> time_end   = microtime(true);
		$this -> duration   = $this -> time_end - $this -> time_start;
		$this -> record();
	}

	/**
	 * Records infos for execution of last SQL statement
	 * Deprecated - does nothing - to be implemented in a better way than this hack
	 */
	private function
	record (): void
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

	/**
	 * Execute any SQL statement
	 *
	 * @param string $sql SQL statement to execute
	 * @param bool $forceExec (opt) Execute in read-only mode (default: do not execute)
	 * @return string|bool if bool, true is succes, false is failure
	 *                     if string, '' is success, else is error message
	 * @todo fix inconsistencies in return type
	 * @throws fwpdoException
	 */
	public function
	executeSql (string $sql, string $dummy='', bool $forceExec = false)
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

			throw new fwpdoException($this -> msg);
		}
		else
			$this -> doRecord ($sql);
		return true;
	}

	private function
	must_executeSql (string $sql)
	{
		try {
			$this -> executeSql($sql);
		} catch (Exception $e) {
			die($e -> getMessage());
		}
	}

	/**
	 * @throws fwpdoException
	 */
	private function
	fetch (string $sql)
	{
		$this -> startSql ($sql);
		try
		{
			$this -> success = false;
			$st = $this->pdo->query($sql); // returns a PDOStatement
			$this->nrows = $st -> rowCount();
			$arr = array();
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

		throw new fwpdoException($this -> msg);
	}

	/**
	 * @throws fwpdoException
	 */

	private function
	buildArgsFromOldSyntax (array $oldArgs, array $parms): array
	{
		$num_args = count ($oldArgs);
		if ($num_args > count ($oldArgs))
			throw new fwpdoException('too many parameters');

		$newArgs = array();
		$argi = 0;
		foreach ($parms as $parm)
			if ($argi < $num_args)
				$newArgs[$parm] = $oldArgs[$argi++];
			else
				$newArgs[$parm] = '';

		return $newArgs;
	}

	public function
	use (string $dbname): void
	{
		$sql = 'USE ' . $dbname;
		$this -> executeSql ($sql, '', true);
	}

	public function
	prepare (string $verb, array $parms): PDOstatement
	{
		switch ($verb)
		{
		case 'select' :
			return $this -> prepareSelect($parms);
		case 'update' :
			return $this -> prepareUpdate($parms);
		case 'insert' :
			return $this -> prepareInsert($parms);
		case 'delete' :
			return $this -> prepareDelete($parms);
		default :
			throw new fwpdoException ('unknown statement: ' . $verb);
		}
	}

	//--------------------------------------------------------------------
	//                            COUNT
	//--------------------------------------------------------------------

	/**
	 * @throws fwpdoException
	 */

	public function
	count ($fromparm, $whereparm = '', $joinparm = '', $dummy=''): int
	{
		if (is_array ($fromparm))	// new syntax
			$args = $fromparm;
		else
		{
			$args = array();
			$args['where'] = $whereparm;
			$args['from']  = $fromparm;
			$args['join']  = $joinparm;
			$args['boolop']  = 'AND';
		}
		$args['select'] = 'COUNT(*) NB';
		$args['limit']   = '';
		$args['orderby'] = '';
		$args['order']   = '';

		$rows = $this -> select ($args);
//tracelog ('[fwpdo::count] ' . $this->sql);
		if (!$rows || $rows == '' || count($rows) < 1)
			throw new fwpdoException('No result');
		return $rows[0]['NB'];
	}

	public function
	must_count ($fromparm, $whereparm = '', $joinparm = ''): int
	{
		try {
			 $this -> count ($fromparm, $whereparm, $joinparm);
		} catch (Exception $e) {
			die($e -> getMessage());
		}
	}

	//--------------------------------------------------------------------
	//                         DESCRIBE / SHOW COLUMNS
	//--------------------------------------------------------------------

	// describe ('table')
	// describe ([ 'from' => 'table', 'where' => [ ... ] );
	/**
	 * @throws fwpdoException
	 */
	public function
	describe ()
	{
		$num_args = func_num_args();

		if ($num_args  != 1)
			throw new fwpdoException('too many parameters');

		$args = func_get_arg(0);
		if (!is_array( $args ))
			$args = [ 'from' => $args ];
		$args = $this -> addMissing ($args, [ 'where' ], '' );
		$args = $this -> addMissing ($args, [ 'boolop' ], 'AND' );

		if (!array_key_exists ('from', $args) || $args['from'] == '')
			throw new fwpdoException('missing table');

		$whereStatement   = $this -> buildWhereStatement ($args['where'], $args['boolop']);
		$fromStatement    = $args['from'];
		$sql = $this -> concatStrings(  'SHOW COLUMNS FROM', $fromStatement, $whereStatement );

		if ($sql == '')
			throw new fwpdoException('unable to build SQL statement');

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
		$parms = [  'select', 'from', 'where', 'orderby', 'limit', 'join', 'order', 'groupby' ];
		return $this -> buildArgsFromOldSyntax ($oldArgs, $parms);
	}

	/**
	 * @throws fwpdoException
	 */
	public function
	select1 (array $pdoparms): ?array
	{
		$rows = $this -> select ($pdoparms);
		if ($this -> num_rows () == 0)
			return null;
		if ($this -> num_rows() > 1)
			throw new fwpdoException ('select1: mutiple results');
		return $rows[0];
	}

	private function
	prepareSelect (array $parms): PDOstatement
	{
		$sql = $this -> buildSelectRequest ($args);
		$st = $this -> pdo -> prepare ($sql);
		return $st;
	}

	/**
	 * @throws fwpdoException
	 */
	// select() accepts various syntaxes and argument types
	// 1- an array (best choice)
	// select( [ 'select' => '*', 'from' => 'table', 'where' => ['field'=>1] ] )
	// 2- a string (when option 1 is not an option)
	// select ('SELECT * FROM table WHERE field=1')
	// 3- a list of arguments (for compatibility only, deprecated)
	// select ('*', 'table', [ 'field' => 1 ]);

	public function
	select ()
	{
		$num_args = func_num_args();

		if ($num_args  == 0)
			throw new fwpdoException('missing parameter');

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
				throw new fwpdoException('wrong parameters');
			$sql = $this -> buildSelectRequest($args);
		}

		if ($sql == '')
			return '';

		$rows = $this->fetch ($sql);
		return $rows;
	}

	/**
	 * @throws fwpdoException
	 */
	public function
	select_sql (string $sql): array
	{
		$result = $this -> fetch ($sql);
		if (is_array($result))
			return $result;
		throw new fwpdoException ('fwpdo::select_sql failed for ' . $sql . ': ' . $this->shortmsg);
	}

	// for compatibility only
	public function
	must_select ($selectparm, $fromparm, $whereparm='', $orderItems="", $limitstr="", $joinparm = '', $sortorder = '', $groupparm = '')
	{
		try
		{
			$this->select($selectparm, $fromparm, $whereparm, $orderItems, $limitstr, $joinparm, $sortorder, $groupparm);
		} catch (Exception $e) {
			die($e -> getMessage());
		}
		return true;
	}

	// for compatibility only
	public function
	must_select_sql (string $sql)
	{
		try
		{
			$this -> fetch ($sql);
		} catch (Exception $e) {
			die($e -> getMessage());
		}
	}

	/**
	 * Builds a SELECT statement based on parameters
	 * @param array $args Parms
	 * @return string SQL statement for reading (SELECT)
	 */
	public function
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
			throw new fwpdoException('missing FROM');
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

		if ($selectStatement == '')
			throw new fwpdoException('empty SELECT FROM');

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

	/**
	 * @throws fwpdoException
	 */
	private function
	buildTablesList ($tablesParm): string
	{
		if (is_array ($tablesParm))
			$tables = implode (',' , $tablesParm );
		else
			$tables = $tablesParm;

		if ($tables == '')
			throw new fwpdoException ('No table provided');

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
	 * @return bool true if success, calls errorhandler if fails
	 */

	/**
	 * Issue a DELETE request (new syntax)
	 * @param array $parms (from, where, limit)
	 * @return bool true if success, calls errorhandler if fails
	 * @throws fwpdoException
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

		$opts = [  'from', 'where', 'limit' ] ;

		if ($num_args  == 1)	// new syntax
			$args = $this -> addMissing (func_get_arg(0), $opts);
		else // old syntax
			$args = $this -> buildArgsFromOldSyntax(func_get_args(), $opts);

		$sql = $this -> buildDeleteRequest ($args);
		if ($sql == '')
			throw new fwpdoException('failed to build SQL statement');

		$this -> executeSql ($sql);
		return true;
	}

	private function
	prepareDelete (array $parms): PDOstatement
	{
		$sql = $this -> buildDeleteRequest ($args);
		$st = $this -> pdo -> prepare ($sql);
		return $st;
	}

	// this is a new function, it does not implements compatibility mode with multiple args
	// if it cannot delete 1 record (no record matches where clause for example), it will return an error
	// this differs from the behaviour of delete()
	/**
	 * @throws fwpdoException
	 */
	public function
	delete1(array $args): bool
	{
		$opts = [  'from', 'where' ] ;
		$args['limit'] = 1;
		$args = $this -> addMissing ($args, $opts);

		$this -> delete($args);
		if ($this -> nrows != 1)
		{
			$this -> success = false;
			throw new fwpdoException('no record found');
		}
		return true;
	}

	// shorthand
	public function
	deleteOrDie ($table, $whereset)
	{
		try
		{
			$this -> delete ($table, $whereset);
		} catch (Exception $e) {
			die ($e -> getMessage() );
		}
	}

	/**
	 * @throws fwpdoException
	 */
	public function
	deleteSql (array $sql, string $dummy=''): bool
	{
		$this -> executeSql ($sql);
		return true;
	}

	public function
	must_delete_sql (array $sql)
	{
		try
		{
			$this -> delete_sql ($sql);
		} catch (Exception $e) {
			die ($e -> getMessage() );
		}
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

	/**
	 *  Start/Stop recording requests
	 *  @param bool $truefalse start if true, stop if false
	 */
	public function
	setRecording (bool $truefalse = true): void
	{
		$this -> recording = true;
		$this -> resetRecorder();
	}

	/**
	 *  Record a statement
	 *  @param string $sql Statement to record
	 */
	public function
	doRecord (string $sql): void
	{
		$this -> sqls[] = $sql;
	}

	/**
	 *  Get recorded statements
	 *  @return array Array of statements as strings
	 */
	public function
	getRecorded(): array
	{
		return $this -> sqls;
	}

	/**
	 *  Reset recording buffer
	 */
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

	/**
	 *  Start a transaction
	 *  In readonly mode, will do nothing
	 *  To avoid nesting of transactions, if a transaction in progress, beginTransaction() increments a count
	 *  commit()/rollback() decrements the count
	 *  and transaction is closed when count reaches 0  
	 */
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
	 * Commit current transaction
	 * @return bool true if ok, false if transaction caanot be committed or no transaction in progress
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
					throw new fwpdoException('commit failed');
			}
			catch (PDOException $e)
			{
				throw new fwpdoException('commit failed');
			}
			$this -> transactionCount = 0;
			return true;
		}

		// if transactionCount == 0, no transaction has been started
		return false;
	}

	/**
	 *  Rollback current transaction
	 *
	 *  @return bool true if ok, false if transaction cannot be rollbacked or no transaction in progress
	 */
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
				throw new fwpdoException('rollback failed');
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
		$parms = [ 'from', 'fields', 'where', 'trimall'
		, 'limit', 'join' ];

		$num_args = func_num_args();

		if ($num_args  == 1)	// new syntax
			$args = $this -> addMissing (func_get_arg(0), $parms);
		else // old syntax
			$args = $this -> buildArgsFromOldSyntax (func_get_args(), $parms);

		$sql = $this -> buildUpdateRequest ($args);
		if ($sql == '')
			throw new fwpdoException('empty SQL');

		$this -> executeSql ($sql);

		return true;
	}

	private function
	prepareUpdate (array $parms): PDOstatement
	{
		$sql = $this -> buildUpdateRequest ($args);
		$st = $this -> pdo -> prepare ($sql);
		return $st;
	}

	public function
	must_update (string $table, array $fields, $whereset, bool $trimall=false)
	{
		return $this -> update ($table, $fields, $whereset, $trimall);
	}

	/**
	 * Issue an UPDATE statement
	 * @param string $sql SQL statement
	 * @param boolean $trimall IF true, will trim fields (default: false)
	 * @return boolean True if sucessful, false is failed
	 */
	public function
	update_sql (string $sql, bool $trimall = false, string $dummy=''): bool
	{
		$this -> executeSql ($sql);
		return true;
	}

	public function
	must_update_sql (string $sql, bool $trimall = false)
	{
		try
		{
			$this -> update_sql ($sql, $trimall);
		} catch (Exception $e) {
			die ($e -> getMessage());
		} 
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
	 * Insert a single record and return its ID
	 * @param string $table Database table
	 * @param array $fields Array of columns (name=>value)
	 * @param boolean $trimall Trim before insert if true (default: no)
	 * @return int ID of inserted element, 0 if failed
	 * Note: not suitable for inserting more than 1 record
	 * Note: not suitable when values are the result of a request
	 * Example :
	 * $id = $pdo -> insert ([
	 * 'from' => $tableName, // mandatory
	 * 'fields' => [ 'field1' => $value1, 'field2' => $value2 ] // mandatory
	 * 'trimall' => true,	// optional - default is FALSE
	 * ]);
	 * if ($id == 0)
	 *   echo $pdo -> getLastError();
	 * else
	 *   echo 'OK';
	 */
	public function
	insert (): int
	{
		$num_args = func_num_args();

		if ($num_args  == 1)	// new syntax
			$args = func_get_arg(0);
		else // old syntax
			$args = $this -> buildArgsFromOldInsertSyntax(func_get_args());

		$sql = $this -> buildInsertRequest ($args);
		
		return $this -> insert_sql ($sql);
	}

	private function
	prepareInsert (array $parms): PDOstatement
	{
		$sql = $this -> buildInsertRequest ($args);
		$st = $this -> pdo -> prepare ($sql);
		return $st;
	}

	private function
	buildArgsFromOldInsertSyntax (array $args): array
	{
		return $this -> buildArgsFromOldSyntax($args, ['from','fields','trimall']);
	}

	/**
	 * Insert a single record from an SQL statement
	 * @param string $sql SQL statement
	 * @return int ID of inserted element, 0 if failed
	 */
	public function
	insert_sql (string $sql, string $dummy=''): int
	{
		if (!$this -> executeSql($sql))
			return 0;

		return $this -> lastInsertId();
	}

	/**
	 * Builds and INSERT statement
	 * @param array $args 'table', 'from', 'fields', 'where', 'trimall'
	 * @return string
	 */
	private function
	buildInsertRequest (array $args): string
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

	/**
	 * Return last inserted id
	 * @param string $name (opt)
	 * @return int ID or 0 if no INSERT performed
	 */
	public function
	lastInsertId (string $name = null): int
	{
		return $this -> pdo -> lastInsertId ($name);
	}

	/**
	 * Format a value according to its type
	 * @param string|int|bool Value to format
	 * @return string Valeur ready to be used in a statement
	 */
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

	/**
	 * Indicates if a string starts with a pattern
	 * @parm string $str haystack
	 * @parm string $pattern needle
	 * @parm bool $case_sentitive Optional - default: yes
	 * @return bool true if string starts with pattern, false otherwise
	 */
	private function
	startsWith (string $str, string $pattern, bool $case_sensitive = true): bool
	{
		if ($case_sensitive)
			return ( substr ($str, 0, strlen($pattern)) == $pattern );
		else
			return ( strcasecmp (substr ($str, 0, strlen($pattern)) , $pattern ) == 0);
	}

	/**
	 * Concatenates strings provided as string and/or array of strings
	 * @return string concatenated string
	 * Examples :
	 * concatStrings('aa', 'bb') => 'aabb'
	 * concatStrings('aa', 'bb', 'cc', 'dd') => 'aabbccdd'
	 * concatStrings('aa', [ 'bb', 'cc' ], 'dd') => 'aabbccdd'
	 * concatStrings( [ ] ) => ''
	 */
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
	public function must_delete ($table, $whereset) { try { $this -> deleteOrDie($table, $whereset); } catch (Exception $e) { die($e -> getMessage()); } }
	public function delete_sql ($sql, $dummy='') { return $this -> deleteSql ($sql); }
	public function must_insert_sql (string $sql) { try { $this -> insert_sql ($sql); } catch (Exception $e) { die($e -> getMessage()); } }
	public function must_insert (string $table, array $fields, bool $trimall=false) { try { $this -> insert ($table, $fields, $trimall); } catch (Exception $e) { die ($e->getMessage()); } }
	public function setwarnchannel($channel) { }

	public function
	sql_build_select ($selectarr, $fromarr, $wherearr, $orderarr, $limitstr,$joinarr, $sortorder, $grouparr, $havingarr)
	{
		$parms = [  'select', 'from', 'where', 'orderby', 'limit', 'join', 'order', 'groupby', 'having' ];
		$newArgs = $this -> buildArgsFromOldSyntax (func_get_args(), $parms);
		$sql = $this -> buildSelectRequest ($newArgs);
		return $sql;
	}
	public function setHandler() { }
}
?>
