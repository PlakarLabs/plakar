import styled from "styled-components";
import { Chip } from "@mui/material";

const Badge = styled.div`
  display: flex;
  flex-direction: row;
  align-items: flex-start;
  justify-content: flex-start;
`;

const BadgeBase = styled.div`
  border-radius: 100px;
  background-color: #fff;
  border: 1px solid #294b7a;
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: center;
  padding: 2px 6px;
  gap: 2px;
`;

const Text1 = styled.div`
  position: relative;
  line-height: 16px;
  font-weight: 600;
`;

const Tag = () => {
return (<Chip color="primary" variant="outlined" label="Text"></Chip>
  );
};

export default Tag;
