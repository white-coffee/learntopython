#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""
 @author: shadow<my_white_coffee@163.com>
 @version: 2017-09-01
"""

import asyncio
import aiomysql


async def create_pool(**kw):
	global __pool
	_pool = await aiomysql.create_pool(
		host=kw.get('host', 'localhost'),
		port=kw.get('port', '3306'),
		user=kw['user'],
		pwd=kw['password'],
		db=kw['db'],
		charset=kw.get('charset', 'utf8'),
		autocommit=kw.get('autocommit', True),
		maxsize=kw.get('maxsize', 10),
		minsize=kw.get('minsize', 1)
	)
