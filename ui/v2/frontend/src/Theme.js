import { createTheme as materialCreateTheme } from '@mui/material/styles';

const font='Inter';

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
  },
  typography: {
    fontFamily: {font},
    fontSize: 16,
    fontWeight: '600',
  },
  spacing: 8,
  direction: 'rtl',
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
    },
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
        background: '#fffa',
        primary: '#33e',
    },
    space: [0, 4, 8, 16, 32, 64, 128, 256, 512],
};

export const materialTheme = materialCreateTheme(themeOptions);