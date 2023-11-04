export const triggerDownload =
    (path, filename) => {
        const link = document.createElement('a');
        link.href = path
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    }
export const copyToClipboard = (text) => {
    navigator.clipboard.writeText(text);
}