import React from 'react';
import DefaultLayout from "./DefaultLayout";
import {Stack} from "@mui/material";
import ConfigShield from "../components/ConfigShield";

function TwoColumnLayout({leftComponent, rightComponent, conf}) {
    return (
        <ConfigShield>
            <DefaultLayout conf={conf}>
                <Stack sx={{p: 2, height: '100%'}} direction={'row'}>
                    <Stack sx={{mr: 1, backgroundColor: 'white', p: 2, borderRadius: 2, width: '70%'}}>
                        {leftComponent}
                    </Stack>
                    <Stack sx={{ml: 1, backgroundColor: 'white', p: 2, borderRadius: 2, width: '30%'}}>
                        {rightComponent}
                    </Stack>
                </Stack>
            </DefaultLayout>
        </ConfigShield>
    )
}

export default TwoColumnLayout;
