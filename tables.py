

def sql_tables(tableName):
    '''
    return SQL statement by table name
    '''
    tracer = "create table IF NOT EXISTS`%s`(\
    `id` int(10) NOT NULL AUTO_INCREMENT,\
    `src` varchar(47) DEFAULT NULL,\
    `dst` varchar(47) DEFAULT NULL,\
    `path_id` int(10) unsigned DEFAULT NULL,\
    `stop_reason` varchar(50) DEFAULT NULL,\
    `ip` varchar(47) DEFAULT NULL,\
    `rtt` double(16,5) DEFAULT NULL,\
    `hop_id` int(10) unsigned DEFAULT NULL,\
    `province` varchar(50) DEFAULT NULL,\
    `city` varchar(50) DEFAULT NULL,\
    `district` varchar(50) DEFAULT NULL,\
    `detail` varchar(50) DEFAULT NULL,\
    `isp` varchar(200) DEFAULT NULL,\
    PRIMARY KEY (`id`),\
    KEY `index_dst` (`dst`),\
    KEY `index_city` (`city`),\
    KEY `index_pathid` (`path_id`)\
    ) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8"

    relation = "create table if not exists `%s`(\
    `id` int(10) NOT NULL AUTO_INCREMENT,\
    pcs varchar(255) null,\
    scs varchar(255) null,\
    delay double(16,5) null,\
    PRIMARY KEY (`id`)\
    ) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8"
    # successor
    # precursor

    ip = "CREATE TABLE `%s` (\
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,\
  `ip` varchar(39) DEFAULT NULL,\
  PRIMARY KEY (`id`)\
) ENGINE=MyISAM AUTO_INCREMENT=348966 DEFAULT CHARSET=utf8"

    geo = "\
    CREATE TABLE IF NOT EXISTS`%s` (\
`id` int(10) unsigned NOT NULL AUTO_INCREMENT,\
`ip` varchar(39) DEFAULT NULL,\
`lm` varchar(39) DEFAULT NULL,\
`lm_mode` varchar(50) DEFAULT NULL,\
`geo_mode` varchar(50) DEFAULT NULL,\
`city` varchar(50) DEFAULT NULL,\
`province` varchar(50) DEFAULT NULL,\
`geo_city` varchar(50) DEFAULT NULL,\
`geo_province` varchar(50) DEFAULT NULL,\
`lm_isp` varchar(200) DEFAULT NULL,\
`geo_isp` varchar(200) DEFAULT NULL,\
`score` tinyint(3)  DEFAULT NULL,\
`error` float unsigned DEFAULT NULL,\
PRIMARY KEY (`id`)\
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8\
    "
    if tableName.endswith('ip'):
        return ip % (tableName)
    if tableName.startswith("tracer"):
        return tracer % (tableName)
    if tableName.startswith("relation"):
        return relation % (tableName)
    if tableName.startswith("geo"):
        return geo % (tableName)
