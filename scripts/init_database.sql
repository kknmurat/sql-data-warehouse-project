/*
Create Database and Schemas 

Script Purpose :
    This script creates a new database named 'dw'. Additionally, the script sets up three schemas
    within the database: 'bronze', 'silver', 'gold'

WARNING:
    Running this script will create a new database and schemas.
    Please proceed with caution.
*/

create database dw;

create schema if not exists bronze;
create schema if not exists silver;
create schema if not exists gold;

