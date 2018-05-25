import * as fs from "fs";
import * as gulp from "gulp";
import * as shell from "gulp-shell";

const contents = fs.readFileSync("./package.json").toString();

const npmPackage = JSON.parse(contents);

const version = npmPackage.version;
const repo = npmPackage.dockerRepository;
const imageName = npmPackage.dockerImageName || npmPackage.name;

const dockerRepoImage = `${repo}/${imageName}`;

const imageWithVersion = `${dockerRepoImage}:${version}`;
const imageAsLatest = `${dockerRepoImage}:latest`;

const buildCommand = `docker build --tag ${imageWithVersion} .`;
const tagCommand = `docker tag ${imageWithVersion} ${imageAsLatest}`;

const pushCommand = `docker push ${imageWithVersion}`;
const pushLatestCommand = `docker push ${imageAsLatest}`;

const cleanCommand = `rm -rf dist`;

const compileTypescript = `tsc -p tsconfig.prod.json`;

const moveMigration = `cp ./server/data-access/migrations/*.ts ./dist/server/data-access/migrations/`;
const moveFiles = `cp ./{package.json,yarn.lock,.sequelizerc,LICENSE,docker-entry.sh,migrate.sh,seed.sh} dist`;
const moveMigrations = `cp -R sequelize-migrations dist/`;

gulp.task("default", ["docker-build"]);

gulp.task("docker-release", ["docker-push"]);

gulp.task("build", shell.task([
        cleanCommand,
        compileTypescript,
        moveFiles,
        moveMigrations,
        moveMigration
    ])
);

gulp.task("docker-build", ["build"], shell.task([
        buildCommand,
        tagCommand
    ])
);

gulp.task("docker-push", ["docker-build"], shell.task([
        pushCommand,
        pushLatestCommand
    ])
);
