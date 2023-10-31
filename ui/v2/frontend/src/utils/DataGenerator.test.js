import {render, screen} from '@testing-library/react';
import {
    createDummySnapshot,
    createDummySnapshotItems, createDummySnapshotPageData, fetchSnapshotPage,
    generateDirectoryPath,
    generateRandomFilePath
} from "./DataGenerator";

test('test generating a directory', () => {
    let path = generateDirectoryPath('bob');
    console.log(path)
    path = generateDirectoryPath();
    console.log(path)
    path = generateDirectoryPath();
    console.log(path)


    // expect not empty
    expect(path).not.toBe('');
});

test('test generating a dummy snapshot', () => {
    const snapshot = createDummySnapshot();

    console.log(snapshot);
    // expect not empty
    expect(snapshot).not.toBe('');
});

test('test generating a dummy snapshot page', () => {
    const repo = createDummySnapshotItems(12);
    const page1 = fetchSnapshotPage(repo, 1, 5);
    const page2 = fetchSnapshotPage(repo, 2, 5);
    const page3 = fetchSnapshotPage(repo, 3, 5);

    console.log(page1);
    // expect not empty
    expect(page1).not.toBe('');
});