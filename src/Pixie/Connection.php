<?php namespace Pixie;

use PDO;
use Pixie\QueryBuilder\Raw;
use Viocon\Container;

class Connection
{

    /**
     * @var Container
     */
    protected $container;

    /**
     * @var string
     */
    protected $adapter;

    /**
     * @var array
     */
    protected $adapterConfig;

    /**
     * @var PDO
     */
    protected $pdoInstance;

    /**
     * @var Connection
     */
    protected static $storedConnection;

    /**
     * @var EventHandler
     */
    protected $eventHandler;

    /**
     * Unix ts of last query
     * @var
     */
    protected $last_query;

    /**
     * @param               $adapter
     * @param array         $adapterConfig
     * @param null|string   $alias
     * @param Container     $container
     */
    public function __construct($adapter, array $adapterConfig, $alias = null, Container $container = null)
    {
        $container = $container ?: new Container();

        $this->container = $container;

        $this->setAdapter($adapter)->setAdapterConfig($adapterConfig)->connect();

        // Create event dependency
        $this->eventHandler = $this->container->build('\\Pixie\\EventHandler');

        if ($alias) {
            $this->createAlias($alias);
        }
    }

    /**
     * Create an easily accessible query builder alias
     * @param $alias
     */
    public function createAlias($alias)
    {
        class_alias('Pixie\\AliasFacade', $alias);
        $builder = $this->container->build('\\Pixie\\QueryBuilder\\QueryBuilderHandler', [$this]);
        AliasFacade::setQueryBuilderInstance($builder);
    }

    /**
     * Returns an instance of Query Builder
     */
    public function getQueryBuilder()
    {
        return $this->container->build('\\Pixie\\QueryBuilder\\QueryBuilderHandler', [$this]);
    }


    /**
     * Create the connection adapter
     */
    public function connect()
    {
        // Close old connection
        $this->pdoInstance = null;

        $adapter = '\\Pixie\\ConnectionAdapters\\' . ucfirst(strtolower($this->adapter));
        $adapterInstance = $this->container->build($adapter, [$this->container]);

        $pdo = $adapterInstance->connect($this->adapterConfig);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        $this->setPdoInstance($pdo);

        // Preserve the first database connection with a static property
        if (!static::$storedConnection) {
            static::$storedConnection = $this;
        }

        $this->last_query = time();
    }

    /**
     * Set reconnect_timeout as connection option to fix timeout troubles like:
     * MySQL error 2006: mysql server has gone away
     * Works only if reconnect_timeout is set
     */
    public function maybeReconnect()
    {
        if (empty($this->adapterConfig['reconnect_timeout']) || empty($this->last_query)) {
            return;
        }

        if (time() - $this->last_query >= $this->adapterConfig['reconnect_timeout']) {
            $this->connect();
        }

        $this->last_query = time();
    }

    /**
     * @param PDO $pdo
     * @return $this
     */
    public function setPdoInstance(PDO $pdo)
    {
        $this->pdoInstance = $pdo;

        return $this;
    }

    /**
     * @return PDO
     */
    public function getPdoInstance()
    {
        return $this->pdoInstance;
    }

    /**
     * @param $adapter
     * @return $this
     */
    public function setAdapter($adapter)
    {
        $this->adapter = $adapter;

        return $this;
    }

    /**
     * @return string
     */
    public function getAdapter()
    {
        return $this->adapter;
    }

    /**
     * @param array $adapterConfig
     * @return $this
     */
    public function setAdapterConfig(array $adapterConfig)
    {
        $this->adapterConfig = $adapterConfig;

        return $this;
    }

    /**
     * @return array
     */
    public function getAdapterConfig()
    {
        return $this->adapterConfig;
    }

    /**
     * @return Container
     */
    public function getContainer()
    {
        return $this->container;
    }

    /**
     * @return EventHandler
     */
    public function getEventHandler()
    {
        return $this->eventHandler;
    }

    /**
     * @return Connection
     */
    public static function getStoredConnection()
    {
        return static::$storedConnection;
    }
}
