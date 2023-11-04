import {AppBar, Container, Link, Stack, Typography} from "@mui/material";
import {Link as RouterLink} from "react-router-dom";
import React from "react";
import CssBaseline from "@mui/material/CssBaseline";
import {ThemeUIProvider} from 'theme-ui';
import {
    THEME_ID,
    ThemeProvider as MaterialThemeProvider
} from '@mui/material/styles';
import {materialTheme, themeUITheme} from '../Theme';
import {connect} from "react-redux";
import {confApp, selectConf} from "../state/Root";
import {ReactComponent as Logo} from '../Logo/Full.svg';

function DefaultLayout({children, conf}) {

    return (
        <>

            <ThemeUIProvider theme={themeUITheme}>
                <MaterialThemeProvider theme={{[THEME_ID]: materialTheme}}>
                    <CssBaseline/>
                    <Stack height={'100vh'}>
                        <AppBar position="static" color="transparent">
                            <Link component={RouterLink} to={'/snapshot'} underline={'none'}>
                                <Stack direction={{xs: 'column', sm: 'row'}}
                                       justifyContent="left"
                                       alignItems="center"
                                       maxWidth="xl" sx={{padding: 2}}>

                                    <Logo padding="s"/>
                                    <Typography href="#"
                                                sx={{padding: 1}}>on <strong>{conf.storeName ? conf.storeName : 'loading...'}</strong></Typography>
                                </Stack>
                            </Link>
                        </AppBar>
                        {children}
                    </Stack>
                </MaterialThemeProvider>
            </ThemeUIProvider>
        </>
    );
}


const mapStateToProps = state => ({
    conf: selectConf(state),
});

const mapDispatchToProps = {
    confApp,
};

export default connect(mapStateToProps, mapDispatchToProps)(DefaultLayout);