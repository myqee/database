<?php

namespace MyQEE\Database;

/**
 * 数据库事务核心类
 *
 * @author     呼吸二氧化碳 <jonwang@myqee.com>
 * @category   Database
 * @copyright  Copyright (c) 2008-2016 myqee.com
 * @license    http://www.myqee.com/license.html
 */
abstract class Transaction
{
    /**
     * 唯一ID
     * @var $id string
     */
    protected $id;

    /**
     * 数据库驱动
     * @var Driver_MySQLI_Factory
     */
    protected $driver;

    public function __construct($dbDriver)
    {
        $this->driver = $dbDriver;
    }

    /**
     * 开启事务
     * @return boolean 是否开启成功
     * @throws \Exception
     */
    abstract public function start();

    /**
     * 提交事务，支持子事务
     *
     * @return Boolean true:成功；false:失败
     */
    abstract public function commit();

    /**
     * 撤消事务，支持子事务
     *
     * @return bool true:成功；false:失败
     */
    abstract public function rollback();

    /**
     * 是否还在事务中
     * @return boolean true=是，false=否
     */
    abstract public function isRoot();

    /**
     * 事务查询
     * @param string $sql
     */
    protected function query($sql)
    {
        try
        {
            if ($this->driver->query($sql, null, true))
            {
                $status = true;
            }
            else
            {
                $status = false;
            }
        }
        catch (\Exception $e)
        {
            $status = false;
        }

        return $status;
    }
}