import {parsePathId} from "./DemoRepo";


test('test generating a dummy snapshot page', () => {
    const {snapshotId, path, filename} =  parsePathId('778819393:/Users/poolp/Documents/Photos/20200101/small.jpeg');
    const {snapshotId: sn1, path: p1, filename: f1} =  parsePathId('778819393:/Users/poolp/Documents/Photos/20200101/');
    console.log(snapshotId);
});