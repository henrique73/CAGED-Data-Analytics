const csv = require('csv-parser')
const fs = require('fs')
// const {MongoClient} = require('mongodb');

const headersDictionary={competência: "date",região:"region",uf:"uf",município:"county",seção:"section",subclasse:"subclass",saldomovimentação:"balance",categoria:"category_code",cbo2002ocupação:"occupation_code",graudeinstrução:"education_degree",idade:"age",sexo:"gender_code",tipoempregador:"employer_type",tipoestabelecimento:"establishment_type",tipomovimentação:"movement_type",indtrabintermitente:"intermittent_worker_indicator",indtrabparcial:"part_worker_indicator",salário:"salary",horascontratuais:"contractual_hours",tipodedeficiência:"disability_code",raçacor:"color_race_code",indicadoraprendiz:"apprentice_indicator",fonte:"source"}
// const uri = "";
// const client = new MongoClient(uri);

// try {
//     client.connect();
//
// } catch (e) {
//     console.error(e);
// }

function average(array){
    var total = 0;
    for(var i = 0; i <array.length;i++){
        total = total + Number(array[i]);
    }
    return (total/array.length);
}

function readFiles(month){

    var inputFolder = "data/" + month + "/"

    console.log("Reading files from " + month);

    fs.readdir("./data/"+month, function (err, files) {

        if (err) throw err;

        console.log(files.length + " files to read...");

        var disabilityHired = [0,0,0,0,0,0,0,0,0,0,0,0];var disabilityFired = [0,0,0,0,0,0,0,0,0,0,0,0]
        var genderMovement = {"10":{ 'male': 0, 'female': 0}, "20": { 'male': 0, 'female': 0}, "25": { 'male': 0, 'female': 0}, "31": { 'male': 0, 'female': 0}, "32": { 'male': 0, 'female': 0}, "33": { 'male': 0, 'female': 0}, "35": { 'male': 0, 'female': 0}, "40": { 'male': 0, 'female': 0}, "43": { 'male': 0, 'female': 0}, "45": { 'male': 0, 'female': 0}, "50": { 'male': 0, 'female': 0}, "60": { 'male': 0, 'female': 0}, "70": { 'male': 0, 'female': 0}, "80": { 'male': 0, 'female': 0}, "90": { 'male': 0, 'female': 0}, "98": { 'male': 0, 'female': 0}, "99": { 'male': 0, 'female': 0}}
        var genderCodeHired = [0,0,0,0,0]; var genderCodeFired = [0,0,0,0,0];
        var MaleSalaryHired = new Array(); var FemaleSalaryHired = new Array();
        var MaleSalaryFired = new Array(); var FemaleSalaryFired = new Array();

        var input;
        for(var i = 0; i < files.length; i++){
            input = fs.createReadStream(inputFolder + files[i]);
            fs.createReadStream("data/"+month+"/"+files[i])
                .pipe(csv({separator: ';',mapHeaders:({header,index}) => {return headersDictionary[header]}}))
                .on('data', (data) => {
                    //Hired Male by salary
                    if(data.movement_type == 10 || data.movement_type == 20 || data.movement_type == 25 || data.movement_type == 35 || data.movement_type == 70){
                        if(data.gender_code == 1){
                            MaleSalaryHired.push(data.salary)
                        }
                    }
                    //Hired Female by salary
                    if(data.movement_type == 10 || data.movement_type == 20 || data.movement_type == 25 || data.movement_type == 35 || data.movement_type == 70){
                        if(data.gender_code == 3){
                            FemaleSalaryHired.push(data.salary)
                        }
                    }

                    //Fired Male by salary
                    if(data.movement_type == 31 || data.movement_type == 32 || data.movement_type == 33 || data.movement_type == 40 || data.movement_type == 43 || data.movement_type == 45 || data.movement_type == 50 || data.movement_type == 60 || data.movement_type == 80 || data.movement_type == 90 || data.movement_type == 98){
                        if(data.gender_code == 1){
                            MaleSalaryFired.push(data.salary)
                        }
                    }
                    //Fired Female by salary
                    if(data.movement_type == 31 || data.movement_type == 32 || data.movement_type == 33 || data.movement_type == 40 || data.movement_type == 43 || data.movement_type == 45 || data.movement_type == 50 || data.movement_type == 60 || data.movement_type == 80 || data.movement_type == 90 || data.movement_type == 98){
                        if(data.gender_code == 3){
                            FemaleSalaryFired.push(data.salary)
                        }
                    }

                    //Type of movement by Gender
                    if(data.gender_code == 1){
                        genderMovement[data.movement_type]['male']++;
                    }
                    if(data.gender_code == 3){
                        genderMovement[data.movement_type]['female']++;
                    }
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
                    console.log("Fired Male : " + genderCodeFired[1] + " Fired Female: "+ genderCodeFired[3] + " Fired Unendentified : " + genderCodeFired[4])
                    // console.log("Male Admission by first job : " + genderMovement[10]['male'])
                    console.log("Male Hired average Salary : " + average(MaleSalaryHired) + " Female Hired average Salary : " + average(FemaleSalaryHired))
                    console.log("Male Fired average Salary : " + average(MaleSalaryFired) + " Female Fired average Salary : " + average(FemaleSalaryFired))
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