Kuidas database tööle saada?

1.  Jooksuta terminalis 'docker compose up'

2.  Seejärel, et PGadmin-i lehele sisse saada, tee oma lemmik browser lahti (nt chrome) ja sisesta sinna oma localhost
    aadress (nt 192.168.1.10) ja compose.yaml-st leitud port (nt 5111). Kokku peaks tulema näiteks 192.168.1.1:5050.

    Kui sa ei tea oma localhost ip-d, siis ava windowsi command prompt ja kirjuta sinna 'ipconfig'.
    Seejärel otsi Wireless Lan adapter Wifi alt IPv4 aadress. "compose.yaml" failist saad leida pordi pgadmini alt.

3.  Kui kõik õigesti tegid, siis peaks tulema pgAdmini leht lahti. Et siseneda vaata username ja password
    compose.yaml failist.

4.  Järgmisena tuleb lisada uus server. Selleks vajuta "Add New Server". Esiteks tuleb lisada serveri nimi, vali
    mis iganes.

5.  Samast menüüst vali ülevalt "Connection" ning sisesta "Host name/address" alla oma ip. Port,
    username ja password peavad olema samad, mis on kirjas "compose.yaml" failis postgres-i juures.

6.  Vajuta save ning peaks tekkima vasakule serverite alla sinu tekitatud server. Selle sees on Database, kuhu tuleks
    luua uus database. Parem klikkiga "Database" peale saab sinna luua uue database-i. Nimi PEAB olema sama, mille oled
    kirjutanud "database.py" faili, muidu ei saa ühendust. Vaata "src/database.py".

7.  Nüüd saab proovida koodi jooksutada. Mine "main.py" ja jooksuta koodi. Kood on hetkel aeglane, sest h-indeksi
    otsimine võtab meeletult aega.

    Kui pole erroreid, siis mine vaata pgAdmin leheküljel andmeid. Selleks vasakult browserist vajuta your_database->
    ->Schemas->public->Tables. Seal peaks olema tekkinud tabelid andmetest. Parem klikkiga ja "view/edit data" alt saab
    vaadata andmeid.

8.  Kui tahad töö ära lõpetada kirjuta terminali "docker compose down" ja kõik.
