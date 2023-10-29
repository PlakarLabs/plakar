import React from 'react';
import {useEffect} from 'react';
import {connect} from 'react-redux';
import {ThemeUIProvider} from 'theme-ui';
import {
    THEME_ID,
    ThemeProvider as MaterialThemeProvider
} from '@mui/material/styles';
import {materialTheme, themeUITheme} from './Theme';
import {selectConf, confApp} from './state/Root';
import {AppBar, Stack, Typography} from "@mui/material";
import {ReactComponent as Logo} from './Logo/Full.svg';




// http://localhost:3000?api_url=http://localhost:8000&store_name=plakar

function App({conf, confApp}) {
    // const apiUrl = useSelector(state => state.root.apiUrl);

    useEffect(() => {
        // Get the URLSearchParams object from the current URL
        const searchParams = new URLSearchParams(window.location.search);

        // Get the value of the 'api' parameter
        const api = searchParams.get('api_url');
        const storeName = searchParams.get('store_name');

        // Store the 'api' value in local storage
        if (api != null && storeName != null) {
            confApp(api, storeName);
            const newUrl = window.location.pathname + window.location.hash;
            window.history.replaceState({}, '', newUrl);
        }
        // Remove the 'api_url' parameter from the URL

    }, [confApp]);


    return (

        <ThemeUIProvider theme={themeUITheme}>
            <MaterialThemeProvider theme={{[THEME_ID]: materialTheme}}>
                <Stack>
                    <AppBar position="static" color="transparent">
                        <Stack direction={{xs:'column', sm: 'row'}} 
                          justifyContent="left"
                          alignItems="center"
                        maxWidth="xl" sx={{padding: 2}}>
                            <Logo padding="s"/>
                            <Typography href="#" sx={{padding: 1}}>on <strong>{conf.storeName ? conf.storeName : 'loading...'}</strong></Typography>
                        </Stack>
                    </AppBar>
                    <Typography>Loading...</Typography>


                </Stack>
            </MaterialThemeProvider>
        </ThemeUIProvider>
    );
}

const mapStateToProps = state => ({
    conf: selectConf(state),
});

const mapDispatchToProps = {
    confApp,
};

export default connect(mapStateToProps, mapDispatchToProps)(App);


// export default App;