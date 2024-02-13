import {faker} from '@faker-js/faker';

export function generateDirectoryPath(userName = faker.internet.userName()) {
    const directoryPath = faker.system.directoryPath()
    if (faker.datatype.boolean()) {
        return directoryPath;
    } else {
        const words = faker.lorem.words({min: 1, max: 10}).split(' ');
        //add /home and username to the words list
        words.unshift(userName)
        words.unshift('home');
        return '/' + words.join('/');
    }
}

export function generateRandomFilePath(userName) {
    // Generate random file properties
    const fileName = faker.system.commonFileName();
    const fileDate = faker.date.anytime();
    const directoryPath = faker.system.directoryPath()

    // randomly return a directory or a file
    if (faker.datatype.boolean()) {
        return directoryPath;
    } else {
        // Generate random file path
        const filePath = `/home/${userName}/Downloads/${fileDate.toISOString()}/${fileName}`;
        return filePath;
    }
}


export function createDummySnapshot() {
    const id = faker.string.uuid();
    const shortId = id.split('-')[0];
    const fileDate = faker.date.anytime().toISOString();
    const userName = faker.internet.userName();
    const hostName = faker.internet.domainName();
    const rootPath = generateDirectoryPath(userName)
    const fileSize = faker.number.int({min: 0, max: 1000}).toLocaleString();
    const sizeAbreviation = faker.helpers.arrayElement(['B', 'KB', 'MB', 'GB', 'TB']);
    const os = faker.helpers.arrayElement(['linux', 'windows', 'mac']);
    // hash signature
    const signature = faker.string.uuid();


    // random array of words
    const words = faker.lorem.words({min: 1, max: 10}).split(' ');

    return {
        id: id,
        shortId: shortId,
        username: userName,
        hostName: hostName,
        location: `${userName}@${hostName}`,
        rootPath: rootPath,
        date: fileDate,
        size: `${fileSize} ${sizeAbreviation}`,
        tags: words,
        os: os,
        signature: signature,
    };
}

export function createDummySnapshotItems(repoSize) {
    // create pageSize dummy items
    return Array.from(Array(repoSize).keys()).map((i) => {
        return createDummySnapshot();
        // return {id, data};
    });
}


export function fetchSnapshotPage(repo, page, pageSize) {
    const totalItems = repo.length;
    const totalPages = Math.ceil(totalItems / pageSize);
    const hasPreviousPage = page > 1;
    const hasNextPage = page < totalPages;
    const items = repo.slice((page - 1) * pageSize, page * pageSize);


    return {
        page: page,
        pageSize: pageSize,
        totalItems: totalItems,
        totalPages: totalPages,
        hasPreviousPage: hasPreviousPage,
        hasNextPage: hasNextPage,
        items: items
    };
}