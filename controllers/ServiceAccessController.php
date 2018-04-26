<?php
	require_once ("../controllers/ModulesLoader.php");
	require_once ("../models/PageCodification.php");

	session_start();
	$modulesLoader = new ModulesLoader();
	$DatabaseController = $modulesLoader -> getDatabaseController();

	function redirectMainPage($errorMessage) {
		$_SESSION['service_pages/Login.php']['error'] = $errorMessage;
		header('Location: ../views/SystemManager.php?selectPage='. ($_SESSION['PageCodification'] -> getChave("MainPage.php")));
	}

	if(!$DatabaseController) {
		redirectMainPage("No database to connect in");
	}

	if(isset($_GET['logout'])){
		if($_GET['logout']){
			session_destroy();
			header('Location: ../views/SystemManager.php');
		}
	}
	if(isset($_SESSION['username']) and isset($_SESSION['password'])){
		$accesUserLevel = $DatabaseController -> validateUser($_SESSION['username'], $_SESSION['password']);
		if($accesUserLevel){
			$_SESSION['logged'] = true;
			$_SESSION['userLevel'] = $accesUserLevel;
			header('Location: ../views/SystemManager.php');
		}
		else{
			redirectMainPage("Username and Password does not match!");
		}
	}
?>