import {AppBar, Link, Skeleton, Stack, Typography} from "@mui/material";
import {Link as RouterLink} from "react-router-dom";
import React from "react";
import CssBaseline from "@mui/material/CssBaseline";
import {ThemeUIProvider} from 'theme-ui';
import {
    THEME_ID,
    ThemeProvider as MaterialThemeProvider
} from '@mui/material/styles';
import {materialTheme, themeUITheme} from '../Theme';
import {confApp, selectConf, selectSnapshotsPage} from "../state/Root";
import {ReactComponent as Logo} from '../Logo/Full.svg';
import {connect} from "react-redux";

function DefaultLayout({children, conf, snapshots}) {

    return (
        <>
            <ThemeUIProvider theme={themeUITheme}>
                <MaterialThemeProvider theme={{[THEME_ID]: materialTheme}}>
                    <CssBaseline/>
                    <Stack height={'100vh'}>
                        <AppBar position="static" color="transparent">
                            <Link component={RouterLink} to='/' underline={'none'}>
                                <Stack direction={{xs: 'column', sm: 'row'}}
                                       justifyContent="left"
                                       alignItems="center"
                                       maxWidth="xl" sx={{padding: 2}}>

                                    <Logo padding="s"/>
                                    <Typography href="#"
                                                sx={{padding: 1}}>on <strong>{conf.repository ? conf.repository :
                                        <Skeleton/>}</strong></Typography>
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
    snapshots: selectSnapshotsPage(state),
});

const mapDispatchToProps = {
    confApp,
};

export default connect(mapStateToProps, mapDispatchToProps)(DefaultLayout);