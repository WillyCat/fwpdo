<?php declare(strict_types=1);

/**
 * fwpdo - Classe d'abstraction de base de données (MySQL, PGSQL, MSSQL)
 * Version 2.0.0 (Modernisée pour PHP 8.2+)
 */
class fwpdoException extends Exception {}

class fwpdoStatement
{
	public function __construct(
		private fwpdo $pdo,
		private PDOStatement $st
	) {}

	public function execute(?array $values = null): bool
	{
		return $this->st->execute($values);
	}

	public function getPDOStatement(): PDOStatement
	{
		return $this->st;
	}
}

class dsn
{
	private string $engine = '';
	private string $database = '';
	private string $host = '';
	private string $socket = '';
	private int $port = 0;
	private string $charset = '';

	public function __construct(array $args)
	{
		foreach (['engine', 'host', 'database'] as $parameter) {
			if (!isset($args[$parameter]) || $args[$parameter] === '') {
				throw new InvalidArgumentException("The parameter '{$parameter}' is mandatory");
			}
		}

		$this->engine = (string) $args['engine'];
		$this->host = (string) $args['host'];
		$this->database = (string) $args['database'];
		$this->socket = (string) ($args['socket'] ?? '');
		$this->charset = (string) ($args['charset'] ?? '');
		$this->port = (int) ($args['port'] ?? 0);

		if (!in_array($this->engine, ['mysql', 'pgsql', 'mssql'], true)) {
			throw new InvalidArgumentException("Engine not supported: [{$this->engine}]");
		}

		if ($this->engine === 'mssql') {
			$this->engine = 'dblib';
		}
	}

	public function __toString(): string
	{
		return $this->build();
	}

	private function build(): string
	{
		$dsnItems = [];

		if (!($this->engine === 'dblib' && $this->database === '<default>')) {
			$dsnItems['dbname'] = $this->database;
		}

		foreach (['host', 'charset'] as $parameter) {
			if ($this->$parameter !== '') {
				$dsnItems[$parameter] = $this->$parameter;
			}
		}

		if ($this->port !== 0) {
			$dsnItems['port'] = $this->port;
		}

		if ($this->socket !== '') {
			$dsnItems['unix_socket'] = $this->socket;
		}

		$dsnparts = [];
		foreach ($dsnItems as $name => $value) {
			$dsnparts[] = "{$name}={$value}";
		}

		return $this->engine . ':' . implode(';', $dsnparts);
	}
}

class dbCnxPool
{
	private static ?array $dbLinks = null;
	private static string $lastError = '';
	private static int $errorCode = 0;

	private function __construct() {}

	public static function getPDO(array $args): ?PDO
	{
		if (self::$dbLinks === null) {
			self::$dbLinks = [];
		}

		if (!isset($args['database'])) {
			throw new InvalidArgumentException('Missing database name');
		}

		$database = $args['database'];
		if (isset(self::$dbLinks[$database])) {
			return self::$dbLinks[$database]['pdo'];
		}

		$dsnObj = new dsn($args);
		$dsn = (string) $dsnObj;

		$opts = [];
		if (!empty($args['persistent'])) {
			$opts[PDO::ATTR_PERSISTENT] = true;
		}

		$engine = $args['engine'] ?? 'mysql';

		try {
			switch ($engine) {
				case 'mysql':
				case 'dblib':
				case 'mssql':
					$pdo = new PDO($dsn, $args['username'], $args['passwd'], $opts);
					break;
				case 'pgsql':
					$dsn .= ';user=' . $args['username'];
					$dsn .= ';password=' . $args['passwd'];
					if (!isset($args['port'])) {
						$dsn .= ';port=5432';
					}
					$pdo = new PDO($dsn, null, null, $opts);
					break;
				default:
					throw new fwpdoException('Engine not implemented: ' . $engine);
			}
		} catch (PDOException $e) {
			self::$errorCode = (int) $e->getCode();
			self::$lastError = $e->getMessage() . ', dsn=' . $dsn;
			return null;
		}

		$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
		$pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, false);
		$pdo->setAttribute(PDO::ATTR_STRINGIFY_FETCHES, false);

		self::$dbLinks[$database] = $args;
		self::$dbLinks[$database]['dsn'] = $dsn;
		self::$dbLinks[$database]['pdo'] = $pdo;

		return $pdo;
	}

	public static function getLastError(): string
	{
		return self::$lastError;
	}

	public static function close(string $database): void
	{
		if (isset(self::$dbLinks[$database])) {
			self::$dbLinks[$database]['pdo'] = null;
			unset(self::$dbLinks[$database]);
		}
	}

	public static function closeAll(): void
	{
		if (self::$dbLinks !== null) {
			foreach (array_keys(self::$dbLinks) as $database) {
				self::close($database);
			}
		}
	}
}

enum fwpdoPrepareMode
{
	case FWPDO_PREPARE_NO;
	case FWPDO_PREPARE_EMULATED;
	case FWPDO_PREPARE_NATIVE;
}

class fwpdo
{
	private string $host = '';
	private string $database = '';
	private string $engine = '';
	private int $transactionCount = 0;
	private bool $transactionFailure = false;
	private float $time_start = 0.0;
	private float $time_end = 0.0;

	public float $duration = 0.0;
	public string $sql = '';
	public bool $success = true;
	public int $nrows = 0;
	public string $shortmsg = '';
	public string $msg = '';

	private int $errorCode = 0;
	private bool $readOnly = false;
	private bool $recording = false;
	private array $sqls = [];
	private int $slow_query_threshold = 0;
	private string $slow_query_callback = '';

	public array $args = [];
	public array $stats = [];
	public array $counters = [];

	private ?PDO $pdo = null;
	private fwpdoPrepareMode $prepareMode = fwpdoPrepareMode::FWPDO_PREPARE_NO;
	private array $params = [];

	public function __construct(...$passedArgs)
	{
		$num_args = count($passedArgs);

		switch ($num_args) {
			case 4:
			case 5:
			case 6:
			case 7:
			case 8:
				$args = $this->buildArgsFromOldSyntax($passedArgs, ['host', 'username', 'passwd', 'database', 'engine', 'port', 'warn_channel', 'socket']);
				break;
			case 1:
				$args = $passedArgs[0];
				if (!is_array($args)) {
					throw new fwpdoException('Syntax error: argument must be an array');
				}
				break;
			default:
				throw new fwpdoException('fwpdo::__construct : unexpected args count: ' . $num_args);
		}

		$this->recording = false;
		$this->readOnly = false;
		$this->resetCounters();

		$args = $this->addMissing($args, ['engine'], 'mysql');
		if (($args['engine'] ?? '') === '') {
			$args['engine'] = 'mysql';
		}
		$this->engine = $args['engine'];

		if (isset($args['database']) && $args['database'] !== '') {
			$this->database = $args['database'];
		} else {
			switch ($this->engine) {
				case 'mssql':
				case 'dblib':
					$this->database = $args['database'] = '<default>';
					break;
				case 'pgsql':
					$this->database = $args['database'] = 'postgres';
					break;
				case 'mysql':
					throw new fwpdoException('Missing database name');
			}
		}

		$this->host = $args['host'] ?? 'localhost';
		$this->pdo = dbCnxPool::getPDO($args);
		$this->args = $args;

		if ($this->pdo === null) {
			throw new fwpdoException(dbCnxPool::getLastError());
		}
	}

	public function resetCounters(): void
	{
		$this->counters = ['SEL' => 0, 'INS' => 0, 'UPD' => 0, 'DEL' => 0, 'TRA' => 0];
	}

	public static function getVersion(): string
	{
		return '2.0.0';
	}

	public function getUUID(): string
	{
		$row = $this->select1('SELECT uuid() AS UUID');
		return $row === null ? '' : (string) $row['UUID'];
	}

	public function prepareSelect(string|array $pdoparms): ?fwpdoStatement
	{
		$sql = $this->buildSelectRequest($pdoparms);
		if ($sql === '')
			return null;

		return new fwpdoStatement($this, $this->pdo->prepare($sql));
	}

	public function setPrepareMode(fwpdoPrepareMode $prepareMode): void
	{
		$this->prepareMode = $prepareMode;
	}

	public function getLastError(bool $long = true): string
	{
		return $long ? $this->msg : $this->shortmsg;
	}

	public function getErrorCode(): int
	{
		return $this->errorCode;
	}

	public function dateTime(?int $timestamp = null): string
	{
		date_default_timezone_set('Europe/Paris');
		return date('Y-m-d H:i:s', $timestamp ?? time());
	}

	private function formatFieldValue(mixed $fieldValue, bool $trimall = false): string
	{
		if ($fieldValue === null) {
			return 'NULL';
		}

		if ($trimall && is_string($fieldValue)) {
			$fieldValue = trim($fieldValue);
		}

		switch ($this->prepareMode) {
			case fwpdoPrepareMode::FWPDO_PREPARE_EMULATED:
			case fwpdoPrepareMode::FWPDO_PREPARE_NATIVE:
				$paramName = 'p' . (count($this->params) + 1);
				$this->params[$paramName] = $fieldValue;
				return ':' . $paramName;
			case fwpdoPrepareMode::FWPDO_PREPARE_NO:
				return $this->quote($fieldValue);
		}
	}

	private function formatFieldName(string $fieldName): string
	{
		return match ($this->engine) {
			'mysql' => '' . $fieldName . '',
			'pgsql' => $fieldName,
			'mssql', 'dblib' => '[' . $fieldName . ']',
			default => $fieldName,
		};
	}

	public function getDatabase(): string
	{
		return $this->database;
	}

	public function getHost(): string
	{
		return $this->host;
	}

	public function get_host(): string
	{
		return $this->getHost();
	}

	public function getAttribute(int $attribute): mixed
	{
		return $this->pdo->getAttribute($attribute);
	}

	public function numRows(): int
	{
		return $this->nrows;
	}

	public function num_rows(): int
	{
		return $this->numRows();
	}

	public function buildWhereStatement(mixed $whereparm, string $boolop = 'AND'): string
	{
		return $this->buildWhereOrHavingStatement($whereparm, 'WHERE', $boolop);
	}

	private function buildHavingStatement(mixed $havingparm, string $boolop = 'AND'): string
	{
		return $this->buildWhereOrHavingStatement($havingparm, 'HAVING', $boolop);
	}

	private function buildWhereOrHavingStatement(mixed $parm, string $statement, string $boolop): string
	{
		if (empty($parm))
			return '';
		if (is_array($parm)) {
			$arr = $parm;
		} else {
			if (str_starts_with(strtoupper(trim($parm)), $statement)) {
				$parm = substr(trim($parm), strlen($statement));
			}
			$arr = [$parm];
		}
		$sql = '';
		foreach ($arr as $col => $item) {
			$part = '';
			if (is_int($col)) {
				if (is_array($item)) {
					$part = $this->formatFieldName($item['col']) . $item['op'] . $this->formatFieldValue($item['value']);
				} else {
					$part = (string) $item;
				}
			} else {
				if ($item === null) {
					$part = $col . ' IS NULL';
				} else {
					if (is_array($item)) {
						if (array_key_exists('op', $item)) {
							$part = $this->formatFieldName($col) . $item['op'] . $this->formatFieldValue($item['value']);
						} elseif (count($item) > 0) {
							$valuesarray = [];
							foreach ($item as $invalue) {
								$valuesarray[] = $this->formatFieldValue($invalue);
							}
							$part = $col . ' IN (' . implode(',', $valuesarray) . ')';
						}
					} else {
						$part = $col . '=' . $this->formatFieldValue($item);
					}
				}
			}
			if ($part !== '') {
				if ($sql === '') {
					$sql .= " \n" . $statement . ' ';
				} else {
					$sql .= "\n  " . $boolop . ' ';
				}
				$sql .= $part;
			}
		}
		return $sql;
	}

	private function startSql(string $sql): void
	{
		$this->msg = '';
		$this->shortmsg = '';
		$this->nrows = 0;
		$this->duration = 0;
		$this->sql = $sql;
		$this->time_start = microtime(true);
		$this->success = false;
		$cmd = strtoupper(substr($sql, 0, 3));
		$this->stats[$cmd] = ($this->stats[$cmd] ?? 0) + 1;
	}

	private function stopSql(): void
	{
		$this->time_end = microtime(true);
		$this->duration = $this->time_end - $this->time_start;
		if ($this->slow_query_threshold > 0 && $this->duration > $this->slow_query_threshold && function_exists($this->slow_query_callback)) {
			call_user_func($this->slow_query_callback, $this->sql, $this->duration);
		}
	}

	public function setSlowQueriesCallback(int $threshold, string $callback): void
	{
		$this->slow_query_threshold = $threshold;
		$this->slow_query_callback = $callback;
	}

	public function resetSlowQueriesCallback(): void
	{
		$this->slow_query_threshold = 0;
		$this->slow_query_callback = '';
	}

	public function setCharset(string $charset, string $collate = ''): bool
	{
		$charset = str_ireplace('utf-8', 'utf8', strtolower($charset));
		$sqlStatement = 'SET NAMES ' . $charset;
		if ($collate !== '') {
			$sqlStatement .= ' COLLATE ' . $collate;
		}
		return $this->executeSql($sqlStatement);
	}

	public function getCharset(): string
	{
		$row = $this->select1(['select' => '@@character_set_database AS C', 'from' => null]);
		return $row === null ? '' : (string) $row['C'];
	}

	public function getCollation(): string
	{
		$row = $this->select1(['select' => '@@collation_database AS C', 'from' => null]);
		return $row === null ? '' : (string) $row['C'];
	}

	public function executeSql(string $sql, string $dummy = '', bool $forceExec = false): bool
	{
		if ($this->recording) {
			$this->sqls[] = $sql;
		}
		if ($forceExec || !$this->readOnly) {
			$this->startSql($sql);
			try {
				$this->nrows = $this->pdo->exec($sql);
				$this->success = true;
			} catch (PDOException $e) {
				$this->errorCode = (int) $e->getCode();
				$this->shortmsg = $e->getMessage();
				$this->msg = $sql . ': ' . $e->getMessage();
			} finally {
				$this->stopSql();
			}
			if ($this->success)
				return true;
			throw new fwpdoException($this->shortmsg);
		}
		return true;
	}

	private function fetch(string $sql): array
	{
		$this->startSql($sql);
		try {
			switch ($this->prepareMode) {
				case fwpdoPrepareMode::FWPDO_PREPARE_NO:
					$st = $this->pdo->query($sql);
					break;
				case fwpdoPrepareMode::FWPDO_PREPARE_EMULATED:
				case fwpdoPrepareMode::FWPDO_PREPARE_NATIVE:
					$st = $this->pdo->prepare($sql);
					$st->execute($this->params);
					break;
			}
			$arr = $st->fetchAll(PDO::FETCH_ASSOC);
			$this->nrows = $st->rowCount();
			$this->success = true;
		} catch (PDOException $e) {
			$this->errorCode = (int) $e->getCode();
			$this->shortmsg = $e->getMessage();
			$this->msg = $sql . ': ' . $e->getMessage();
			$arr = [];
		} finally {
			$this->stopSql();
		}
		if ($this->success)
			return $arr;
		throw new fwpdoException($this->msg);
	}

	private function buildArgsFromOldSyntax(array $oldArgs, array $parms): array
	{
		$num_args = count($oldArgs);
		$newArgs = [];
		$argi = 0;
		foreach ($parms as $parm) {
			$newArgs[$parm] = ($argi < $num_args) ? $oldArgs[$argi++] : '';
		}
		return $newArgs;
	}

	public function count(string|array $fromparm, string|array $whereparm = '', string|array $joinparm = ''): int
	{
		if (is_array($fromparm)) {
			$args = $fromparm;
		} else {
			$args = ['where' => $whereparm, 'join' => $joinparm, 'boolop' => 'AND'];
			if (!str_contains($fromparm, ' ')) {
				$args['from'] = $fromparm;
			} else {
				$args['from'] = '(' . $fromparm . ') SUBQUERY';
			}
		}
		$args['limit'] = '';
		$args['orderby'] = '';
		$args['order'] = '';
		$sqlSelect = $this->buildSelectRequest($args);
		$sqlCount = 'SELECT COUNT(*) FWCOUNT FROM (' . $sqlSelect . ') XX';
		$rows = $this->fetch($sqlCount);
		return (int) ($rows[0]['FWCOUNT'] ?? 0);
	}

	public function describe(mixed $args)
	{
		if (!is_array($args)) {
			$args = ['from' => $args];
		}
		$args = $this->addMissing($args, ['where'], '');
		$args = $this->addMissing($args, ['boolop'], 'AND');
		if (!isset($args['from']) || $args['from'] === '') {
			throw new fwpdoException('Missing table name');
		}
		$whereStatement = $this->buildWhereStatement($args['where'], $args['boolop']);
		return $this->fetch($this->concatStrings('SHOW COLUMNS FROM', $args['from'], $whereStatement));
	}

	public function select1(string|array $pdoparms): ?array
	{
		$rows = $this->select($pdoparms);
		if ($this->numRows() === 0 || empty($rows))
			return null;
		if ($this->numRows() > 1) {
			throw new fwpdoException('select1: multiple results returned');
		}
		return $rows[0];
	}

	public function select11(string|array $pdoparms): mixed
	{
		$row = $this->select1($pdoparms);
		return $row === null ? null : array_pop($row);
	}

	public function select(...$passedArgs): ?array
	{
		if (empty($passedArgs)) {
			throw new fwpdoException('Missing parameter in select()');
		}
		$this->params = [];
		if (count($passedArgs) === 1) {
			$args = $passedArgs[0];
			$sql = is_array($args) ? $this->buildSelectRequest($args) : $args;
		} else {
			$args = $this->buildArgsFromOldSyntax($passedArgs, ['select', 'from', 'where', 'orderby', 'limit', 'join', 'order', 'groupby']);
			$sql = $this->buildSelectRequest($args);
		}
		if ($sql === '')
			return null;
		$this->counters['SEL']++;
		return $this->fetch($sql);
	}

	public function version(): string
	{
		$arr = $this->fetch('SELECT version() AS version');
		return (string) ($arr[0]['version'] ?? '');
	}

	public function select_sql(string $sql): array
	{
		return $this->fetch($sql);
	}

	public function buildSelectRequest(array $args): string
	{
		$args = $this->addMissing($args, ['select'], '*');
		$args = $this->addMissing($args, ['join', 'where', 'groupby', 'orderby', 'order', 'limit', 'having']);
		$args = $this->addMissing($args, ['from'], 'MISSING');
		$args = $this->addMissing($args, ['boolop'], 'AND');

		if ($args['from'] === 'MISSING') {
			throw new fwpdoException('Missing FROM statement');
		}

		$selectStatement = $this->buildSelectStatement($args['select']);
		$fromStatement = ($args['from'] === null) ? '' : $this->buildFromStatement($args['from']);
		$joinStatements = $this->concatStrings($args['join']);
		$whereStatement = $this->buildWhereStatement($args['where'], $args['boolop']);
		$groupByStatement = $this->buildGroupByStatement($args['groupby']);
		$havingStatement = $this->buildHavingStatement($args['having']);
		$orderByStatement = $this->buildOrderByStatement($args['orderby']);
		$orderStatement = strtoupper((string) $args['order']);
		$limitStatement = $this->buildLimitStatement($args['limit'], 2);

		if ($selectStatement === '') {
			throw new fwpdoException('Empty SELECT clause');
		}

		if ($orderStatement !== '' && $orderByStatement === '') {
			$orderStatement = '';
		}

		return $this->concatStrings($selectStatement, $fromStatement, $joinStatements, $whereStatement, $groupByStatement, $havingStatement, $orderByStatement, $orderStatement, $limitStatement);
	}

	private function buildSelectStatement(mixed $selectParm): string
	{
		$selectParts = is_array($selectParm) ? $selectParm : [$selectParm];
		$statement = '';
		foreach ($selectParts as $column) {
			$statement .= ($statement === '') ? 'SELECT ' : ', ';
			$statement .= $column;
		}

		return $statement;
	}

	private function buildFromStatement(string|array $fromParm): string
	{
		$tables = is_array($fromParm) ? implode(',', $fromParm) : $fromParm;

		if ($tables === '')
			throw new fwpdoException('No table provided in FROM');

		return $this->concatStrings('FROM', $tables);
	}

	private function buildOrderByStatement(string|array $orderParm): string
	{
		$orderItems = is_array($orderParm) ? $orderParm : [$orderParm];
		$statement = '';
		foreach ($orderItems as $orderItem) {
			if (empty($orderItem))
				continue;

			$statement .= ($statement === '') ? " \nORDER BY " : ", \n";
			$statement .= $orderItem;
		}

		return $statement;
	}

	private function buildGroupByStatement(string|array $groupparm): string
	{
		$grouparr = is_array($groupparm) ? $groupparm : [$groupparm];
		$statement = '';
		foreach ($grouparr as $grouppart) {
			if (empty($grouppart))
				continue;

			$statement .= ($statement === '') ? " \nGROUP BY " : ", \n";
			$statement .= $grouppart;
		}

		return $statement;
	}

	private function buildLimitStatement(mixed $limitParm, int $maxParts): string
	{
		if (empty($limitParm))
			return '';

		if (!is_array($limitParm)) {
			$cleaned = str_replace(['LIMIT', ' '], '', strtoupper((string) $limitParm));
			$limitParm = explode(',', $cleaned);
		}

		if ($maxParts === 2 && count($limitParm) === 2) {
			$offset = (int) $limitParm[0];
			$maxrows = (int) $limitParm[1];
		} else {
			$offset = 0;
			$maxrows = (int) $limitParm[0];
		}

		if ($maxrows === 0)
			return '';

		return match ($this->engine) {
			'mysql' => $offset === 0 ? " LIMIT {$maxrows}" : " LIMIT {$offset},{$maxrows}",
			'pgsql', 'mssql', 'dblib' => " OFFSET {$offset} ROWS FETCH NEXT {$maxrows} ROWS ONLY",
			default => '',
		};
	}

	public function delete(...$passedArgs): bool
	{
		$opts = ['from', 'where', 'limit', 'join'];
		$args = (count($passedArgs) === 1 && is_array($passedArgs[0])) ? $this->addMissing($passedArgs[0], $opts) : $this->buildArgsFromOldSyntax($passedArgs, $opts);
		if (is_array($args['from'])) {
			if (count($args['from']) > 1)
				throw new fwpdoException('Only one table should be provided for delete');
			$args['from'] = $args['from'][0] ?? '';
		}

		if (empty($args['from']))
			throw new fwpdoException('Missing table for deletion');

		$fromStatement = $this->buildFromStatement($args['from']);
		$whereStatement = $this->buildWhereStatement($args['where']);
		$limitStatement = $this->buildLimitStatement($args['limit'], 1);
		$joinStatements = $this->concatStrings($args['join']);
		$sql = $this->concatStrings('DELETE', $fromStatement, $joinStatements, $whereStatement, $limitStatement);
		$this->counters['DEL']++;
		$this->executeSql($sql);
		return true;
	}

	public function delete1(array $args): bool
	{
		$args['limit'] = 1;
		$this->delete($args);
		if ($this->nrows !== 1) {
			$this->success = false;
			throw new fwpdoException('No record found to delete');
		}
		return true;
	}

	public function setReadOnly(bool $truefalse = true): void
	{
		$this->readOnly = $truefalse;
	}

	public function setRecording(bool $truefalse = true): void
	{
		$this->recording = $truefalse;
		$this->sqls = [];
	}

	public function getRecorded(): array
	{
		return $this->sqls;
	}

	public function getTransactionCount(): int
	{
		return $this->transactionCount;
	}

	public function beginTransaction(): void
	{
		if ($this->readOnly)
			return;
		$this->transactionCount++;
		if ($this->transactionCount === 1) {
			$this->pdo->beginTransaction();
			$this->sql = 'START TRANSACTION';
			$this->transactionFailure = false;
		}
	}

	public function beginTrans(): void
	{
		$this->beginTransaction();
	}

	public function commit(): bool
	{
		if ($this->readOnly)
			return true;
		if ($this->transactionCount > 1) {
			$this->transactionCount--;
			return true;
		}
		if ($this->transactionCount === 1) {
			if ($this->transactionFailure) {
				$this->rollback();
			} else {
				$this->counters['TRA']++;
				$this->sql = 'COMMIT';
				$this->pdo->commit();
				$this->transactionCount = 0;
				return true;
			}
		}
		return false;
	}

	public function rollback(): bool
	{
		if ($this->readOnly)
			return true;
		if ($this->transactionCount > 1) {
			$this->transactionCount--;
			return true;
		}
		if ($this->transactionCount === 1) {
			$this->sql = 'ROLLBACK';
			$this->pdo->rollback();
			$this->transactionCount = 0;
			return true;
		}
		return false;
	}

	public function update(...$passedArgs): bool
	{
		$parms = ['from', 'fields', 'where', 'trimall', 'limit', 'join'];
		$args = (count($passedArgs) === 1 && is_array($passedArgs[0])) ? $this->addMissing($passedArgs[0], $parms) : $this->buildArgsFromOldSyntax($passedArgs, $parms);
		$tables = $this->buildTablesList($args['from']);
		$whereStatement = $this->buildWhereStatement($args['where']);
		$setArray = [];
		foreach ((array) $args['fields'] as $fieldName => $fieldValue) {
			$setArray[] = $this->formatFieldName($fieldName) . '=' . $this->formatFieldValue($fieldValue, (bool) $args['trimall']);
		}
		$setStatement = implode(',', $setArray);
		$limitStatement = $this->buildLimitStatement($args['limit'], 1);
		$joinStatements = $this->concatStrings($args['join']);
		$sql = $this->concatStrings('UPDATE', $tables, $joinStatements, 'SET', $setStatement, $whereStatement, $limitStatement);
		$this->counters['UPD']++;
		$this->executeSql($sql);
		return true;
	}

	public function insert(...$passedArgs): int
	{
		$args = (count($passedArgs) === 1 && is_array($passedArgs[0])) ? $passedArgs[0] : $this->buildArgsFromOldSyntax($passedArgs, ['from', 'fields', 'trimall']);
		if (array_key_exists('into', $args) && !array_key_exists('from', $args)) {
			$args['from'] = $args['into'];
		}
		$args = $this->addMissing($args, ['from', 'fields']);
		$tables = $this->buildTablesList($args['from']);
		$fieldNameArray = [];
		$fieldValueArray = [];
		foreach ((array) $args['fields'] as $fieldname => $fieldvalue) {
			$fieldNameArray[] = $this->formatFieldName($fieldname);
			$fieldValueArray[] = $this->formatFieldValue($fieldvalue, (bool) ($args['trimall'] ?? false));
		}
		$sql = $this->concatStrings('INSERT INTO', $tables, '(' . implode(',', $fieldNameArray) . ')', 'VALUES', '(' . implode(',', $fieldValueArray) . ')');
		$this->counters['INS']++;
		$this->executeSql($sql);
		return (int) $this->pdo->lastInsertId();
	}

	public function lastInsertId(?string $name = null): int
	{
		return (int) $this->pdo->lastInsertId($name);
	}

	public function quote(mixed $s): string
	{
		if (is_string($s) || is_int($s) || is_float($s)) {
			$cr = $this->pdo->quote((string) $s);
			if ($cr === false) {
				throw new fwpdoException('Cannot quote the variable value');
			}
			return $cr;
		}
		if (is_bool($s)) {
			return $s ? 'TRUE' : 'FALSE';
		}
		if (is_array($s)) {
			throw new fwpdoException('Cannot format an array inside quote()');
		}
		return (string) $s;
	}

	private function concatStrings(...$parms): string
	{
		$result = '';
		foreach ($parms as $parm) {
			if (is_array($parm)) {
				foreach ($parm as $arrayitem) {
					if (is_array($arrayitem)) {
						$result .= ' ' . $this->concatStrings(...$arrayitem);
					} else {
						$result .= ' ' . trim((string) $arrayitem);
					}
				}
			} else {
				$result .= ' ' . trim((string) $parm);
			}
		}
		return trim(preg_replace('/\s+/', ' ', $result));
	}

	protected function addMissing(array $args, array $entries, string $defaultValue = ''): array
	{
		foreach ($entries as $entry) {
			if (!array_key_exists($entry, $args)) {
				$args[$entry] = $defaultValue;
			}
		}
		return $args;
	}

	private function buildTablesList(mixed $tablesParm): string
	{
		$tables = is_array($tablesParm) ? implode(',', $tablesParm) : (string) $tablesParm;
		if ($tables === '')
			throw new fwpdoException('No table provided');
		return $tables;
	}  // --- Rétrocompatibilité Alias ---

	public function set_charset($c): bool
	{
		return $this->setCharset($c);
	}

	public function get_database(): string
	{
		return $this->getDatabase();
	}
}

?>
