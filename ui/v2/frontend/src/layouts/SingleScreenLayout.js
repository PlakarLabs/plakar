import React from 'react';
import DefaultLayout from "./DefaultLayout";
import {Stack} from "@mui/material";

function SingleScreenLayout({children, conf}) {
    return (
        <DefaultLayout conf={conf}>
            <Stack sx={{p: 2}}>
                <Stack sx={{backgroundColor: 'white', p: 2, borderRadius: 2}}>
                    {children}
                </Stack>
            </Stack>
        </DefaultLayout>
    )
};

export default SingleScreenLayout;
