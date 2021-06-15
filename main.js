const csv = require('csv-parser')
const fs = require('fs')
const {MongoClient} = require('mongodb');

const headersDictionary={competência: "date",região:"region",uf:"uf",município:"county",seção:"section",subclasse:"subclass",saldomovimentação:"balance",categoria:"category_code",cbo2002ocupação:"occupation_code",graudeinstrução:"education_degree",idade:"age",sexo:"gender_code",tipoempregador:"employer_type",tipoestabelecimento:"establishment_type",tipomovimentação:"movement_type",indtrabintermitente:"intermittent_worker_indicator",indtrabparcial:"part_worker_indicator",salário:"salary",horascontratuais:"contractual_hours",tipodedeficiência:"disability_code",raçacor:"color_race_code",indicadoraprendiz:"apprentice_indicator",fonte:"source"}
const results = [];
const uri = "mongodb+srv://admin:D8QDgP2twy2hchNY@cluster0.d7gka.mongodb.net/myFirstDatabase?retryWrites=true&w=majority";
const client = new MongoClient(uri);

try {
    client.connect();

} catch (e) {
    console.error(e);
}

function readFiles(month){

    var inputFolder = "data/" + month + "/"

    console.log("Reading files from " + month);

    fs.readdir("./data/"+month, function (err, files) {

        if (err) throw err;

        console.log(files.length + " files to read...");

        var input, request;
        var disabilityHiredPhyisical = 0,disabilityHiredAuditory = 0,disabilityHiredVisual = 0,disabilityHiredIntellectual = 0,disabilityHiredMultiple = 0,disabilityHiredRehabilitated = 0,disabilityHiredUnidentified = 0

        for(var i = 0; i < files.length; i++){

            var result = {table:[]};

            input = fs.createReadStream(inputFolder + files[i]);
            console.log("Reading file" + files[i])
            fs.createReadStream("data/"+month+"/"+files[i])
                .pipe(csv({separator: ';',mapHeaders:({header,index}) => {return headersDictionary[header]}}))
                .on('data', (data) => {
                    if(data.movement_type == 10 || data.movement_type == 20 || data.movement_type == 25 || data.movement_type == 35 || data.movement_type == 70){
                        if(data.disability_code !=0){
                            if(data.disability_code == 1){
                                disabilityHiredPhyisical =  disabilityHiredPhyisical + 1;
                            }
                            if(data.disability_code == 2){
                                disabilityHiredAuditory = disabilityHiredAuditory + 1;
                            }
                            if(data.disability_code == 3){
                                disabilityHiredVisual = disabilityHiredVisual + 1;
                            }
                            if(data.disability_code == 4){
                                disabilityHiredIntellectual = disabilityHiredIntellectual + 1;
                            }
                            if(data.disability_code == 5){
                                disabilityHiredMultiple = disabilityHiredMultiple + 1;
                            }
                            if(data.disability_code == 6){
                                disabilityHiredRehabilitated = disabilityHiredRehabilitated + 1;
                            }
                            if(data.disability_code == 9){
                                disabilityHiredUnidentified = disabilityHiredUnidentified + 1;
                            }
                        }
                    }
                })
                .on('end', () => {console.log(disabilityHiredPhyisical + "|" + disabilityAuditory)});

        }
    });

}

console.time();
readFiles("Abril");


var http = require("http");
http.createServer(function (request, response) {


    response.writeHead(200, {'Content-Type': 'text/plain'});

    response.end("a");
}).listen(8000);

console.log('Server running at http://127.0.0.1:8000/')

/*

	  fs.createReadStream('CAGEDMOV202001.txt')
  		.pipe(csv())
  		.on('data', (data) => results.push(data))
  		.on('end', () => {
    	console.log(console.timeEnd());});



		results.push(data)

*/