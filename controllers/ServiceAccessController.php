<?php
require_once ("../controllers/ModulesLoader.php");
require_once ("../models/PageCodification.php");
require_once ("../models/value/User.class.php");

session_start ();
$modulesLoader = new ModulesLoader ();
$DatabaseController = $modulesLoader->getDatabaseController ();

function redirectMainPage($errorMessage) {
	$_SESSION ['service_pages/Login.php'] ['error'] = $errorMessage;
	header ( 'Location: ../views/SystemManager.php?selectPage=' . ($_SESSION ['PageCodification']->getChave ( "MainPage.php" )) );
}

if (! $DatabaseController) {
	redirectMainPage ( "No database to connect in" );
}

if (isset ( $_GET ['logout'] )) {
	if ($_GET ['logout']) {
		session_destroy ();
		header ( 'Location: ../views/SystemManager.php' );
	}
}
if (isset ( $_SESSION ['username'] ) and isset ( $_SESSION ['password'] )) {
	$accessLevel_codeUser = $DatabaseController->validateUser ( $_SESSION ['username'], $_SESSION ['password'] );
	$accessUserLevel = $accessLevel_codeUser [0];
	$codeUser = $accessLevel_codeUser [1];
	
	if ($accessUserLevel) {
		$_SESSION ['logged'] = new User ( $accessUserLevel, $_SESSION ['username'], $_SESSION ['password'], $codeUser );
		$_SESSION ['userLevel'] = $accessUserLevel;
		header('Location: ../views/SystemManager.php');
	} else {
		redirectMainPage ( "Username and Password does not match!" );
	}
}
?>