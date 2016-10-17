for (var i=0;i<100;i++) {
    console.log(randomIndex(3));
}


function randomIndex(indexSize) {
    return Math.floor((Math.random() * (indexSize)));
}