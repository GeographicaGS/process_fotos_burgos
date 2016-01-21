#!/bin/bash

data=media
rsync -avr -e 'ssh -p 2222' --delete root@mother:/home/sharefolder/ragnarok/proyectos/eiel_burgos/operaciones/fotos_mirador_nueva $data

rm -R  $data/fotos_mirador_nueva/00_squeleton
