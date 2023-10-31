import {createTheme as materialCreateTheme, styled} from '@mui/material/styles';

const font = 'Inter';

const gray = {
    50: '#F9FAFB',
    500: '#6B7280',
    600: '#4B5563',
}

export const themeOptions = {
    palette: {
        mode: 'light',
        primary: {
            main: '#294b7a',
        },
        secondary: {
            main: '#f50057',
        },
        text: {
            primary: '#374151',
        },
        success: {
            main: '#059669',
        },
        gray: {
            50: gray[50],
            500: gray[500],
            600: gray[600],
        },
        background: {
            default: gray[50],
        }

    },
    typography: {
        fontFamily: font,
        fontSize: 16,
        fontWeight: '600',
        textsmregular: {
            fontSize: 14,
            fontStyle: 'normal',
            fontWeight: '400',
            lineHeight: '20px',
        },
        textxsmedium: {
            fontSize: 12,
            fontWeight: 600,
            lineHeight: '16px',
        }
    },
    spacing: 8,
    direction: 'ltr',
    shape: {
        borderRadius: 4,
    },
    overrides: {
        MuiAppBar: {
            colorInherit: {
                backgroundColor: '#689f38',
                color: '#fff',
            },
        },
    },
    props: {
        MuiAppBar: {
            color: 'inherit',
        },
        MuiTooltip: {
            arrow: true,
        },
        MuiList: {
            dense: true,
        },
        MuiMenuItem: {
            dense: true,
        },
        MuiTable: {
            size: 'small',
        },
    },
    components: {
        MuiButtonBase: {
            defaultProps: {
                disableRipple: true,
            },
            styleOverrides: {
                root: {
                    // fontSize: '14px',
                    // fontStyle: 'normal',
                    // fontWeight: 500,
                    // lineHeight: '20px',
                }
            }
        },
        MuiPaginationItem: {
            styleOverrides: {
                root: {
                    fontSize: '14px',
                    fontStyle: 'normal',
                    fontWeight: 500,
                    lineHeight: '20px',
                }
            }
        }
    },
};

// https://zenoo.github.io/mui-theme-creator/#Chip

export const themeUITheme = {
    fonts: {
        body: 'Inter',
        heading: 'Inter',
        monospace: 'Menlo, monospace',
    },
    fontWeights: {
        body: 600,
        heading: 700,
        bold: 800,
    },
    colors: {
        text: '#000',
        background: '#F9FAFB',
        primary: '#33e',
    },
    space: [0, 4, 8, 16, 32, 64, 128, 256, 512],
};


export const materialTheme = materialCreateTheme(themeOptions);

materialTheme.typography.h3 = {
    fontFamily: 'Inter',
    fontSize: '30px',
    fontStyle: 'normal',
    fontWeight: 500,
    lineHeight: '40px',
}