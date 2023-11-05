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

export const OS = {
    MAC: 'Mac OS',
    IOS: 'iOS',
    WINDOWS: 'Windows',
    ANDROID: 'Android',
    LINUX: 'Linux',
    OTHER: 'other'
}

export function getOS() {
    var platform = navigator.platform.toLowerCase();
    var macosPlatforms = ['macintosh', 'macintel', 'macppc', 'mac68k'];
    var windowsPlatforms = ['win32', 'win64', 'windows', 'wince'];
    var iosPlatforms = ['iphone', 'ipad', 'ipod'];
    var os = OS.OTHER

    if (macosPlatforms.indexOf(platform) !== -1) {
        os = OS.MAC;
    } else if (iosPlatforms.indexOf(platform) !== -1) {
        os = OS.IOS;
    } else if (windowsPlatforms.indexOf(platform) !== -1) {
        os = OS.WINDOWS
    } else if (/android/.test(platform)) {
        os = OS.ANDROID;
    } else if (!os && /linux/.test(platform)) {
        os = OS.LINUX;
    }

    return os;
}
