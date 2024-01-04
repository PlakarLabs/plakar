// a function that return the folder name and path pairs from the deepest level to the root
// eg /root -> [ {name: 'root', path: '/root'}]
// eg /root/fred/ -> [ { 'root', '/root'}, { 'fred', '/root/fred'}]

export function getFolderNameAndPathPairs(path) {
  if (!path.endsWith('/')) {
    path += '/';
  }

  const result = [];
  let position = path.length - 1;

  while (position > 0) {
    const lastSlashIndex = path.lastIndexOf('/', position + 1);
    const prevSlashIndex = path.lastIndexOf('/', lastSlashIndex - 1);
    // Extract the folder name
    const folderName = path.substring(prevSlashIndex + 1, lastSlashIndex);
    // Add the folder name and path pair to the result array
    result.unshift({ name: folderName, path: path.substring(0, lastSlashIndex + 1) });
    // Update the position to search for the next "/"
    position = prevSlashIndex;
  }

  return result;
}


export function getDirectoryPath(pathId) {
    return pathId.split('/').slice(0, -1).join('/');
}

export function getFileName(pathId) {
    return pathId.split('/').slice(-1)[0];
}