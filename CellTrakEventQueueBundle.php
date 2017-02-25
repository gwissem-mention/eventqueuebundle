<?php
namespace Celltrak\EventQueueBundle;

use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\HttpKernel\Bundle\Bundle;
use CellTrak\EventQueueBundle\DependencyInjection\CellTrakEventQueueCompilerPass;

class CelltrakEventQueueBundle extends Bundle
{

    public function build(ContainerBuilder $container)
    {
        parent::build($container);
        $container->addCompilerPass(new CellTrakEventQueueCompilerPass);
    }

}
