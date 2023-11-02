import {getFolderNameAndPathPairs} from "./Path";

test('test generating a dummy snapshot page', () => {
    const r = getFolderNameAndPathPairs('/home/fred/bob/')
    expect(r[0]).toEqual({'name': 'home', 'path': '/home/'});
    expect(r[1]).toEqual({'name': 'fred', 'path': '/home/fred/'});
    expect(r[2]).toEqual({'name': 'bob', 'path': '/home/fred/bob/'});
})