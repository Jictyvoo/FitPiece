<?php
    $_SESSION['paginaAnterior'] = "service_pages/Login.php";
?>

<!DOCTYPE html>
<html>

<head>
    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">

    <title>Acesso</title>

    <link rel="stylesheet" href="../layout/css/bootstrap.min.css" media="screen" type="text/css" />

</head>
<link rel="icon" href="../layout/imagens/favicon.ico">

<body class="profile-login">
    
    <?php $_SESSION['paginaAnterior'] = "service_pages/Login.php";?>

    <title>Login</title>
    <div class="row">
        <div class="col-md-2"></div>
        <div class="col-md-8">
            <form  method="post" action="<?='../views/SystemManager.php?selectPage='.($_SESSION['PageCodification'] -> getChave("../controllers/ServiceAccessController.php"))?>">
                <h2 class="form-signin-heading">Login</h2>
                <input id="inputUsername" name="username" class="form-control" placeholder="Nome de Usuário" required autofocus/>
                <input type="password" id="inputPassword" name="password" class="form-control" placeholder="Senha" required/>
                <div class="checkbox">
                    <label>
                        <input type="checkbox" value="remember-me"> Remember Me
                    </label>
                </div>
                <input class="btn btn-lg btn-primary btn-block" type="submit" value="Entrar"/>
            </form>

            <?php if(isset($_SESSION['service_pages/Login.php']['error'])) : ?>
                <br/><br/>
                <div class="alert alert-danger" role="alert">
                    <?= $_SESSION['service_pages/Login.php']['error'] ?>
                </div>
                <?php unset($_SESSION['service_pages/Login.php']['error']); ?>
            <?php endif ?>

        </div>
        <div class="col-md-2"></div>
    </div>
    <br><br><br><br><br>

</body>

</html>