<?php
require_once "../models/PageCodification.php";
require_once "../models/PageInformations.php";
require_once "../controllers/ModulesLoader.php";
session_start();

/*foreach (explode ("/", $_SERVER['REQUEST_URI']) as $part){
echo($part."<br/>");
}*/

if (!isset($_SESSION['PageCodification'])) {
    $informationsAboutPage        = new PageInformations();
    $pageNames                    = $informationsAboutPage->getPageNames();
    $_SESSION['PageCodification'] = new PageCodification(array_merge($pageNames, array("MainPage.php", "../controllers/ServiceAccessController.php")));
    $_SESSION['PageCodification']->associaCodificacaoPagina();
}

if (!isset($_SESSION['selectedDash'])) {
    $_SESSION['selectedDash'] = "Default";
}

$paginaDestino = "MainPage.php";
if (isset($_GET['selectPage'])) {
    $paginaDestino = $_SESSION['PageCodification']->getCodigoPagina($_GET['selectPage']);
}
if (isset($_SESSION['logged'])) {
    $isLogged = $_SESSION['logged'];
    if (!isset($_SESSION['firstAcces'])) {
        $_SESSION['firstAcces'] = true;
    }
} else {
    $isLogged = false;
}
/*Start of page Configuration*/
function makePageTitle() {
    $_SESSION['navbarSelected']       = $GLOBALS['paginaDestino'];
    $GLOBALS['informationsAboutPage'] = new PageInformations();
    $titlePage                        = $GLOBALS['informationsAboutPage']->getTitle($GLOBALS['paginaDestino']);

    return $titlePage;
}

function preparePage() {
    if ($_SESSION['firstAcces']) {
        $GLOBALS['informationsAboutPage'] = new PageInformations();

        if ($GLOBALS['paginaDestino'] == "MainPage.php") {
            $GLOBALS['paginaDestino'] = $GLOBALS['informationsAboutPage']->getDashboardInformations()->getMainPage($_SESSION['userLevel']);
        }

        $_SESSION['PageCodification'] = new PageCodification($GLOBALS['informationsAboutPage']->getDashboardInformations()->getRedirectPages($GLOBALS['informationsAboutPage']->getUserAccessLevel($GLOBALS['paginaDestino'])));

        $_SESSION['PageCodification']->associaCodificacaoPagina();

        $_SESSION['paginaAnterior'] = $GLOBALS['informationsAboutPage']->getDashboardInformations()->getMainPage($GLOBALS['informationsAboutPage']->getUserAccessLevel($GLOBALS['paginaDestino']));

        $_SESSION['navbarSelected'] = $_SESSION['paginaAnterior'];
        $_SESSION['firstAcces']     = false;
        $_SESSION['logout_status']  = false;
    }
}

function getRedirectPage($openPage) {
    return 'SystemManager.php?selectPage=' . $_SESSION['PageCodification']->getChave($openPage);
}

/*End of page Configuration*/

if ($isLogged) {
    if (!isset($_SESSION['username']) || !isset($_SESSION['password'])) {
        header('Location: ../views/SystemManager.php');
    }

    $modulesLoader      = new ModulesLoader();
    $DatabaseController = $modulesLoader->getDatabaseController();
    if (!$DatabaseController) {
        $_SESSION['logged'] = false;
    }
    if ($DatabaseController && !$DatabaseController->validateUser($_SESSION['username'], $_SESSION['password'])) {
        $_SESSION['logged'] = false;
        header('Location: ../views/SystemManager.php');
    }
    preparePage();
    if (!isset($_GET['selectPage'])) {
        $paginaDestino = $_SESSION['paginaAnterior'];
    } else {
        $paginaDestino = $_SESSION['PageCodification']->getCodigoPagina($_GET['selectPage']);
    }

    $titlePage                = makePageTitle();
    $_SESSION['selectedDash'] = $GLOBALS['informationsAboutPage']->getDashboardInformations()->getDashboard($_SESSION['userLevel']);
    $dashLocation             = "../layout/dashboards/" . $_SESSION['selectedDash'] . "/";
    if (!file_exists($dashLocation)) {
        $dashLocation = "../layout/dashboards/Default/Default";
    } else {
        $dashLocation = $dashLocation . $_SESSION['selectedDash'];
    }
    include $dashLocation . "_TOP.php";
    if ($informationsAboutPage->getDefaultLayout($paginaDestino) == $_SESSION['selectedDash'] || $informationsAboutPage->getDefaultLayout($paginaDestino) == "*" || $dashLocation == "../layout/dashboards/Default/Default") {
        include $informationsAboutPage->getFileLocation($paginaDestino);
    } else {
        include "service_pages/404.php";
    }
    include $dashLocation . "_BOT.php";
} else {
    switch ($paginaDestino) {
        case "MainPage.php":
            include "MainPage.php";
            break;
        case "../controllers/ServiceAccessController.php":
            if (isset($_POST['username']) and isset($_POST['password'])) {
                $_SESSION['username'] = $_POST['username'];
                $_SESSION['password'] = $_POST['password'];
                header('Location: ../controllers/ServiceAccessController.php');
            }
            break;
        default:
            $_SESSION['errorFound'] = $_GET['selectPage'];
            include "../layout/dashboards/Default/Default_TOP.php";
            include "service_pages/404.php";
            include "../layout/dashboards/Default/Default_BOT.php";
            break;
    }
}
