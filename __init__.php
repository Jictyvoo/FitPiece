<?php
/**
 * User: João Victor Oliveira Couto
 * Date: 23/05/2018
 * Time: 23:10
 */

require_once "models/business/ClassDAO.php";
require_once "models/business/EncryptDecrypt.php";

require_once "models/value/User.class.php";

require_once "models/DashboardsInformations.php";
require_once "models/PageCodification.php";
require_once "models/PageInformations.php";
require_once "models/SystemConfiguration.php";

require_once "util/ArrayList.php";
require_once "util/Stack.php";
require_once "util/exception/UnsupportedOperationException.php";

require_once "controllers/DatabaseController.php";
require_once "controllers/ModulesLoader.php";