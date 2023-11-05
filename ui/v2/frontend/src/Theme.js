import {createTheme as materialCreateTheme} from '@mui/material/styles';

const font = 'Inter';

export const gray = {
    50: '#F9FAFB',
    100: '#F3F4F6',
    500: '#6B7280',
    600: '#4B5563',
    700: '#3F3F46',
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
        tertiary: {
            main: '#D1F347',
        },
        text: {
            primary: '#374151',
            secondary: '#FFFFFF'
        },
        success: {
            main: '#059669',
        },
        gray: {
            50: gray[50],
            100: gray[100],
            500: gray[500],
        600: gray[600],
            700: gray[700],
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
        },
        textbasemedium: {
            fontSize: 16,
            fontWeight: 500,
            lineHeight: '150%',
        },
        textlgmedium: {
            fontSize: 18,
            fontWeight: 500,
            lineHeight: '28px',
        },
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
                disableRipple: false,
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
        },
        MuiChip: {
            variants: [
                {
                    props: {variant: 'tag'},
                    style: {
                        fontSize: 12,
                        fontWeight: 600,
                    lineHeight: '16px',
                        color: gray[700],
                    },
                },
            ],
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