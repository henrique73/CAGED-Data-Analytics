const csv = require('csv-parser')
const fs = require('fs')
const {MongoClient} = require('mongodb');

const headersDictionary={competência: "date",região:"region",uf:"uf",município:"county",seção:"section",subclasse:"subclass",saldomovimentação:"balance",categoria:"category_code",cbo2002ocupação:"occupation_code",graudeinstrução:"education_degree",idade:"age",sexo:"gender_code",tipoempregador:"employer_type",tipoestabelecimento:"establishment_type",tipomovimentação:"movement_type",indtrabintermitente:"intermittent_worker_indicator",indtrabparcial:"part_worker_indicator",salário:"salary",horascontratuais:"contractual_hours",tipodedeficiência:"disability_code",raçacor:"color_race_code",indicadoraprendiz:"apprentice_indicator",fonte:"source"}
const results = [];
const uri = "";
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

        var disabilityHired = [0,0,0,0,0,0,0,0,0,0,0,0];var disabilityFired = [0,0,0,0,0,0,0,0,0,0,0,0]
        var genderCodeHired = [0,0,0,0,0]; var genderCodeFired = [0,0,0,0,0];
        var totalHired = 0;

        var input, request;
        var disabilityHiredPhyisical = 0,disabilityHiredAuditory = 0,disabilityHiredVisual = 0,disabilityHiredIntellectual = 0,disabilityHiredMultiple = 0,disabilityHiredRehabilitated = 0,disabilityHiredUnidentified = 0
        for(var i = 0; i < files.length; i++){
            input = fs.createReadStream(inputFolder + files[i]);
            fs.createReadStream("data/"+month+"/"+files[i])
                .pipe(csv({separator: ';',mapHeaders:({header,index}) => {return headersDictionary[header]}}))
                .on('data', (data) => {
                    //Hired by type of disability
                    if(data.movement_type == 10 || data.movement_type == 20 || data.movement_type == 25 || data.movement_type == 35 || data.movement_type == 70){
                        if(data.disability_code !=0){
                            disabilityHired[data.disability_code] = disabilityHired[data.disability_code]+1
                        }
                    }
                    //Fired by type of disability
                    if(data.movement_type == 31 || data.movement_type == 32 || data.movement_type == 33 || data.movement_type == 40 || data.movement_type == 43 || data.movement_type == 45 || data.movement_type == 50 || data.movement_type == 60 || data.movement_type == 80 || data.movement_type == 90 || data.movement_type == 98){
                        if(data.disability_code !=0){
                            disabilityFired[data.disability_code] = disabilityFired[data.disability_code]+1
                        }
                    }
                    //Hired by Gender
                    if(data.movement_type == 10 || data.movement_type == 20 || data.movement_type == 25 || data.movement_type == 35 || data.movement_type == 70){
                        totalHired = totalHired+1
                        if(data.gender_code == 9){
                            genderCodeHired[4] = genderCodeHired[4]+1
                        }
                        genderCodeHired[data.gender_code] = genderCodeHired[data.gender_code]+1
                    }
                    //Fired by Gender
                    if(data.movement_type == 31 || data.movement_type == 32 || data.movement_type == 33 || data.movement_type == 40 || data.movement_type == 43 || data.movement_type == 45 || data.movement_type == 50 || data.movement_type == 60 || data.movement_type == 80 || data.movement_type == 90 || data.movement_type == 98){
                        if(data.gender_code == 9){
                            genderCodeFired[4] = genderCodeFired[4]+1
                        }
                        genderCodeFired[data.gender_code] = genderCodeFired[data.gender_code]+1
                    }
                })
                .on('end', () => {
                    console.log("Hired Male : " + genderCodeHired[1] + " Female Hired: "+ genderCodeHired[3] + " Hired Unendentified : " + genderCodeHired[4])
                    console.log("Fired Male : " + genderCodeFired[1] + " Fired Hired: "+ genderCodeFired[3] + " Fired Unendentified : " + genderCodeFired[4])
                });

        }
    });
}

readFiles("Janeiro");
// readFiles("Fevereiro");
// readFiles("Março");
// readFiles("Abril");
// readFiles("Junho");
// readFiles("Julho");
// readFiles("Agosto");
// readFiles("Setembro");
// readFiles("Outubro");
// readFiles("Novembro");
// readFiles("Dezembro");


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