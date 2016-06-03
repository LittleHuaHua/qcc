#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Customized Errors'''
#
# Created by Shengying Pan, 2016
class WindError(Exception):
    '''this is an error to be raised if wind went wrong'''
    def __init__(self, code, message):
        super().__init__()
        self.code = code
        self.message = message
    def __str__(self):
        return 'WIND ERROR: ' +  str(self.code) + ' => ' + self.message

class ServerError(Exception):
    '''this is an error to be raised if server connection failed'''
    def __init__(self, status, message):
        super().__init__()
        self.status = status
        self.message = message
    def __str__(self):
        return 'SERVER ERROR: ' + str(self.status) + ' => ' + self.message